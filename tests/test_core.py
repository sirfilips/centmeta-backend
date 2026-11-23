import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

BACKEND_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BACKEND_DIR not in sys.path:
    sys.path.insert(0, BACKEND_DIR)

import main
import master_scraper
import scryfall_search


class TestMasterScraper(unittest.TestCase):
    def test_normalize_partner_commander_sorts_partners_alphabetically(self):
        self.assertEqual(
            master_scraper.normalize_partner_commander("Tymna the Weaver / Kraum, Ludevic's Opus"),
            "Kraum, Ludevic's Opus / Tymna the Weaver",
        )
        self.assertEqual(
            master_scraper.normalize_partner_commander("Atraxa, Praetors' Voice"),
            "Atraxa, Praetors' Voice",
        )

    def test_run_scraper_locked_ignores_json_import_without_flag(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            db_file = os.path.join(tmpdir, "test_centurion.db")
            json_file = os.path.join(tmpdir, "database_centurion.json")

            master_scraper.init_db(db_file)
            conn = sqlite3.connect(db_file)
            conn.execute(
                "INSERT INTO decks (id_moxfield, comandante, data_aggiornamento) VALUES ('keep1', 'Yoshimaru', '2026-01-01')"
            )
            conn.commit()
            conn.close()

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump([{"id_moxfield": "new1", "comandante": "Overwrite"}], f)

            with (
                patch.object(master_scraper, "db_path", db_file),
                patch.object(master_scraper, "json_path", json_file),
                patch.object(master_scraper, "rate_limited_get", return_value=None),
                patch.object(master_scraper.time, "sleep", return_value=None),
                patch.dict(os.environ, {"MOXFIELD_UA": "CentMetaTest/1.0", "ALLOW_JSON_IMPORT": "0"}, clear=False),
            ):
                result = master_scraper._run_scraper_locked()
                self.assertFalse(result)

            conn = sqlite3.connect(db_file)
            rows = conn.execute("SELECT id_moxfield FROM decks").fetchall()
            conn.close()
            self.assertEqual([r[0] for r in rows], ["keep1"])


class TestClientIpResolution(unittest.TestCase):
    @staticmethod
    def _make_request(peer_host: str, headers: dict):
        return SimpleNamespace(
            client=SimpleNamespace(host=peer_host),
            headers={k.lower(): v for k, v in headers.items()},
        )

    def test_untrusted_peer_ignores_spoofed_headers(self):
        req = self._make_request(
            "198.51.100.25",
            {
                "CF-Connecting-IP": "203.0.113.9",
                "X-Real-IP": "203.0.113.10",
                "X-Forwarded-For": "203.0.113.11",
            },
        )
        self.assertEqual(main.get_client_ip(req), "198.51.100.25")

    def test_trusted_loopback_uses_forwarded_headers(self):
        req_cf = self._make_request(
            "127.0.0.1",
            {"CF-Connecting-IP": "203.0.113.42", "X-Forwarded-For": "198.51.100.1"},
        )
        self.assertEqual(main.get_client_ip(req_cf), "203.0.113.42")

        req_xff = self._make_request(
            "127.0.0.1",
            {"X-Forwarded-For": "203.0.113.7, 127.0.0.1"},
        )
        self.assertEqual(main.get_client_ip(req_xff), "203.0.113.7")


class TestScryfallCacheThreadSafety(unittest.TestCase):
    def test_snapshot_isolation_and_atomic_commit(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            cache_file = os.path.join(tmpdir, "scryfall_cache.json")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({"Lightning Bolt": {"cmc": 1, "type": "Instant"}}, f)

            with (
                patch.object(main, "CACHE_FILE", cache_file),
                patch.object(main, "_SCRYFALL_CACHE_MEMORY", None),
            ):
                snapshot = main.load_scryfall_cache()
                snapshot["MutatedLocal"] = {"cmc": 99}

                fresh_snapshot = main.load_scryfall_cache()
                self.assertNotIn("MutatedLocal", fresh_snapshot)

                updated = main._commit_scryfall_cache_updates(
                    {"Counterspell": {"cmc": 2, "type": "Instant"}},
                    delete_keys=["Lightning Bolt"],
                )
                self.assertIn("Counterspell", updated)
                self.assertNotIn("Lightning Bolt", updated)

                with open(cache_file, "r", encoding="utf-8") as f:
                    on_disk = json.load(f)
                self.assertEqual(list(on_disk.keys()), ["Counterspell"])

    def test_commit_after_cache_clear_preserves_existing_disk_entries(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            cache_file = os.path.join(tmpdir, "scryfall_cache.json")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({"Lightning Bolt": {"cmc": 1, "type": "Instant"}}, f)

            with (
                patch.object(main, "CACHE_FILE", cache_file),
                patch.object(main, "_SCRYFALL_CACHE_MEMORY", None),
            ):
                updated = main._commit_scryfall_cache_updates(
                    {"Counterspell": {"cmc": 2, "type": "Instant"}}
                )
                self.assertIn("Lightning Bolt", updated)
                self.assertIn("Counterspell", updated)

                with open(cache_file, "r", encoding="utf-8") as f:
                    on_disk = json.load(f)
                self.assertIn("Lightning Bolt", on_disk)
                self.assertIn("Counterspell", on_disk)

    def test_parse_scryfall_artist_and_backfill(self):
        single = main._parse_scryfall_card_object({
            "name": "Lightning Bolt",
            "type_line": "Instant",
            "image_uris": {"normal": "https://cards.scryfall.io/normal/front/a/a/11111111-1111-1111-1111-111111111111.jpg", "art_crop": "https://cards.scryfall.io/art_crop/front/a/a/11111111-1111-1111-1111-111111111111.jpg"},
            "oracle_text": "Lightning Bolt deals 3 damage to any target.",
            "artist": "Christopher Moeller",
        })
        self.assertEqual(single["artist"], "Christopher Moeller")

        dfc = main._parse_scryfall_card_object({
            "name": "Delver of Secrets // Insectile Aberration",
            "card_faces": [
                {
                    "name": "Delver of Secrets",
                    "type_line": "Creature — Human Wizard",
                    "image_uris": {"normal": "https://img/f1.jpg", "art_crop": "https://img/a1.jpg"},
                    "oracle_text": "At the beginning of your upkeep...",
                    "artist": "Nils Hamm",
                },
                {
                    "name": "Insectile Aberration",
                    "type_line": "Creature — Human Insect",
                    "image_uris": {"normal": "https://img/f2.jpg", "art_crop": "https://img/a2.jpg"},
                    "oracle_text": "Flying",
                    "artist": "Nils Hamm",
                },
            ],
        })
        self.assertEqual(dfc["artist"], "Nils Hamm")

        bolt_uuid = "11111111-2222-3333-4444-555555555555"
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            cache_file = os.path.join(tmpdir, "scryfall_cache.json")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({
                    "lightning bolt": {
                        "full_name": "Lightning Bolt",
                        "images": [f"https://cards.scryfall.io/normal/front/1/1/{bolt_uuid}.jpg?123"],
                        "art_crops": [f"https://cards.scryfall.io/art_crop/front/1/1/{bolt_uuid}.jpg?123"],
                        "type": "Instant",
                    },
                    "counterspell": {
                        "full_name": "Counterspell",
                        "images": ["https://cards.scryfall.io/normal/front/2/2/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jpg"],
                        "art_crops": ["https://cards.scryfall.io/art_crop/front/2/2/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee.jpg"],
                        "type": "Instant",
                        "artist": "Zack Stella",
                    },
                }, f)

            posted_payloads = []

            class _FakeSession:
                def post(self, url, json=None, timeout=15):
                    posted_payloads.append(json)
                    return SimpleNamespace(
                        status_code=200,
                        json=lambda: {
                            "data": [{
                                "id": bolt_uuid,
                                "name": "Lightning Bolt",
                                "artist": "Christopher Rush",
                            }]
                        },
                    )

            with (
                patch.object(main, "CACHE_FILE", cache_file),
                patch.object(main, "_SCRYFALL_CACHE_MEMORY", None),
                patch.object(main, "get_scryfall_session", return_value=_FakeSession()),
                patch.object(main, "_wait_scryfall_rate_limit", return_value=None),
            ):
                updated_count = main.backfill_scryfall_artists()
                self.assertEqual(updated_count, 1)
                self.assertEqual(posted_payloads, [{"identifiers": [{"id": bolt_uuid}]}])

                reloaded = main.load_scryfall_cache()
                self.assertEqual(reloaded["lightning bolt"]["artist"], "Christopher Rush")
                self.assertEqual(
                    reloaded["lightning bolt"]["art_crops"],
                    [f"https://cards.scryfall.io/art_crop/front/1/1/{bolt_uuid}.jpg?123"],
                )
                self.assertEqual(reloaded["counterspell"]["artist"], "Zack Stella")

    def test_backfill_scryfall_artists_from_bulk_jsonl_gz(self):
        import gzip
        import io

        uuid_target = "11111111-2222-3333-4444-555555555555"
        uuid_other = "99999999-8888-7777-6666-555555555555"

        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            rows = [
                {
                    "id": uuid_other,
                    "name": "Opt",
                    "artist": "Wrong Printing Artist",
                },
                {
                    "id": uuid_target,
                    "name": "Opt",
                    "artist": "John Avon",
                },
                {
                    "id": "aaaaaaaa-0000-0000-0000-000000000001",
                    "name": "Dark Ritual",
                    "artist": "Sandra Everingham",
                },
            ]
            payload = "\n".join(json.dumps(r) for r in rows) + "\n"
            gz.write(payload.encode("utf-8"))
        gz_bytes = buf.getvalue()

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            cache_file = os.path.join(tmpdir, "scryfall_cache.json")
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump({
                    "opt": {
                        "full_name": "Opt",
                        "images": [f"https://cards.scryfall.io/normal/front/1/1/{uuid_target}.jpg?123"],
                        "art_crops": [f"https://cards.scryfall.io/art_crop/front/1/1/{uuid_target}.jpg?123"],
                        "type": "Instant",
                    },
                    "dark ritual": {
                        "full_name": "Dark Ritual",
                        "images": ["https://example.invalid/no-uuid.jpg"],
                        "art_crops": ["https://example.invalid/no-uuid-art.jpg"],
                        "type": "Instant",
                    },
                }, f)

            requested_urls = []

            class _FakeBulkResponse:
                status_code = 200

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def iter_content(self, chunk_size=65536):
                    yield gz_bytes[:30]
                    yield gz_bytes[30:]

            class _FakeBulkSession:
                def get(self, url, stream=False, timeout=15):
                    requested_urls.append(url)
                    if "bulk-data/default-cards" in url:
                        return SimpleNamespace(
                            status_code=200,
                            json=lambda: {
                                "jsonl_download_uri": "https://data.scryfall.io/default-cards/test.jsonl.gz"
                            },
                        )
                    return _FakeBulkResponse()

            with (
                patch.object(main, "CACHE_FILE", cache_file),
                patch.object(main, "_SCRYFALL_CACHE_MEMORY", None),
                patch.object(main, "get_scryfall_session", return_value=_FakeBulkSession()),
                patch.object(main, "_wait_scryfall_rate_limit", return_value=None),
            ):
                targets = main.collect_missing_artist_targets()
                updates = main.backfill_scryfall_artists_from_bulk(targets)
                self.assertEqual(len(updates), 2)
                self.assertEqual(updates["opt"]["artist"], "John Avon")
                self.assertEqual(
                    updates["opt"]["art_crops"],
                    [f"https://cards.scryfall.io/art_crop/front/1/1/{uuid_target}.jpg?123"],
                )
                self.assertEqual(updates["dark ritual"]["artist"], "Sandra Everingham")
                self.assertEqual(
                    requested_urls,
                    [
                        "https://api.scryfall.com/bulk-data/default-cards",
                        "https://data.scryfall.io/default-cards/test.jsonl.gz",
                    ],
                )


class TestMdfcAndDailyCacheFreshness(unittest.TestCase):
    def test_mdfc_spell_land_included_in_mana_curve(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            main_db = os.path.join(tmpdir, "centurion.db")
            conn = sqlite3.connect(main_db)
            conn.execute(
                "CREATE TABLE decks (id_moxfield TEXT PRIMARY KEY, comandante TEXT, data_aggiornamento TEXT)"
            )
            conn.execute(
                "CREATE TABLE deck_cards (deck_id TEXT, card_name TEXT)"
            )
            conn.execute(
                "INSERT INTO decks VALUES ('d1', 'Kraum', '2026-11-10')"
            )
            conn.execute("INSERT INTO deck_cards VALUES ('d1', 'Sink into Stupor')")
            conn.execute("INSERT INTO deck_cards VALUES ('d1', 'Island')")
            conn.commit()
            conn.close()

            def _get_test_db():
                c = sqlite3.connect(main_db, timeout=30)
                c.row_factory = sqlite3.Row
                return c

            fake_scryfall = {
                "sink into stupor": {"cmc": 3, "type": "Instant // Land", "color_identity": ["U"]},
                "island": {"cmc": 0, "type": "Basic Land — Island", "color_identity": ["U"]},
            }

            main.get_global_cards_stats.cache_clear()
            main.get_decks_stats_cached.cache_clear()
            with (
                patch.object(main, "get_db", side_effect=_get_test_db),
                patch.object(main, "load_scryfall_cache", return_value=fake_scryfall),
                patch.object(main, "fetch_scryfall_data", return_value=fake_scryfall),
            ):
                stats = main.get_decks_stats_cached("all", None)
                main.get_global_cards_stats.cache_clear()
                main.get_decks_stats_cached.cache_clear()

            curve = stats["mana_curve"]
            self.assertEqual(curve[3], 1.0)

    def test_ensure_daily_caches_fresh_clears_when_date_changes(self):
        with (
            patch.object(main, "_LAST_CACHE_DATE", "1999-01-01"),
            patch.object(main, "clear_all_caches", wraps=main.clear_all_caches) as spy_clear,
        ):
            main.ensure_daily_caches_fresh()
            self.assertEqual(spy_clear.call_count, 1)
            self.assertEqual(main._LAST_CACHE_DATE, main.datetime.now().strftime("%Y-%m-%d"))


class TestMetagameHistory(unittest.TestCase):
    def test_metagame_history_monthly_and_weekly_buckets(self):
        from datetime import timedelta

        now = main.datetime.now()
        d_old = (now - timedelta(days=45)).strftime("%Y-%m-%dT12:00:00")
        d_new = (now - timedelta(days=2)).strftime("%Y-%m-%dT12:00:00")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            main_db = os.path.join(tmpdir, "centurion.db")
            conn = sqlite3.connect(main_db)
            conn.execute(
                "CREATE TABLE decks (id_moxfield TEXT PRIMARY KEY, comandante TEXT, data_aggiornamento TEXT)"
            )
            decks = [
                ("m1", "Yoshimaru", d_old),
                ("m2", "Yoshimaru", d_old),
                ("m3", "Kraum", d_old),
                ("m4", "Kraum", d_new),
                ("m5", "Kraum", d_new),
                ("m6", "Kraum", d_new),
                ("m7", "Sconosciuto", d_new),
            ]
            conn.executemany("INSERT INTO decks VALUES (?, ?, ?)", decks)
            conn.commit()
            conn.close()

            def _get_test_db():
                c = sqlite3.connect(main_db, timeout=30)
                c.row_factory = sqlite3.Row
                return c

            fake_scryfall = {
                "Yoshimaru": {"cmc": 1, "type": "Legendary Creature", "color_identity": ["W"], "image": "https://img/y.jpg"},
                "Kraum": {"cmc": 5, "type": "Legendary Creature", "color_identity": ["U", "R"], "image": "https://img/k.jpg"},
            }

            main.get_global_commanders_validity.cache_clear()
            main.get_metagame_history_cached.cache_clear()
            with (
                patch.object(main, "get_db", side_effect=_get_test_db),
                patch.object(main, "fetch_scryfall_data", return_value=fake_scryfall),
                patch.object(
                    main,
                    "get_global_commanders_validity",
                    return_value=(frozenset({"Yoshimaru", "Kraum"}), ("Sconosciuto",)),
                ),
                patch.object(main, "get_global_cards_stats", return_value=(6, {})),
                patch.object(main, "get_db_mtime", return_value=1700000000.0),
            ):
                monthly = main.get_metagame_history_cached("month", 6, 5)
                weekly = main.get_metagame_history_cached("week", 6, 5)
                main.get_metagame_history_cached.cache_clear()
                main.get_global_commanders_validity.cache_clear()

            self.assertEqual(monthly["summary"]["total_decks"], 6)
            self.assertEqual(monthly["summary"]["unique_commanders"], 2)
            self.assertGreaterEqual(len(monthly["buckets"]), 2)
            self.assertEqual(monthly["top_commanders"][0]["name"], "Kraum")
            self.assertEqual(weekly["granularity"], "week")
            self.assertGreaterEqual(len(weekly["buckets"]), 2)


class TestScryfallRateLimitPacing(unittest.TestCase):
    def test_wait_scryfall_rate_limit_spaces_calls(self):
        import inspect

        default_interval = inspect.signature(main._wait_scryfall_rate_limit).parameters["min_interval"].default
        self.assertGreaterEqual(default_interval, 0.5)

        with patch.object(main, "_scryfall_last_request_ts", 0.0):
            t0 = main.std_time.monotonic()
            main._wait_scryfall_rate_limit(0.05)
            main._wait_scryfall_rate_limit(0.05)
            elapsed = main.std_time.monotonic() - t0
            self.assertGreaterEqual(elapsed, 0.04)


class TestRoleClassificationAndScryfallSearch(unittest.TestCase):
    def test_classify_card_roles_accuracy_and_exclusions(self):
        bolt = {
            "type": "Instant",
            "oracles": ["Lightning Bolt deals 3 damage to any target."],
        }
        self.assertIn("burn", scryfall_search.classify_card_roles(bolt))
        self.assertIn("removal", scryfall_search.classify_card_roles(bolt))

        brainstorm = {
            "type": "Instant",
            "oracles": ["Draw three cards, then put two cards from your hand on top of your library in any order."],
        }
        self.assertIn("card_draw", scryfall_search.classify_card_roles(brainstorm))

        sheoldred = {
            "type": "Legendary Creature — Phyrexian Praetor",
            "oracles": ["Deathtouch\nWhenever you draw a card, you gain 2 life.\nWhenever an opponent draws a card, they lose 2 life."],
        }
        self.assertNotIn("card_draw", scryfall_search.classify_card_roles(sheoldred))

        wasteland = {
            "type": "Land",
            "oracles": ["{T}: Add {C}.\n{T}, Sacrifice this land: Destroy target nonbasic land."],
        }
        self.assertEqual(scryfall_search.classify_card_roles(wasteland), [])

        mdfc_bounce = {
            "type": "Instant // Land",
            "oracles": [
                "✦ Sink into Stupor ✦\nReturn target spell or nonland permanent you don't control to its owner's hand.",
                "✦ Soporific Springs ✦\nAs this land enters, you may pay 3 life. If you don't, it enters tapped.\n{T}: Add {U}.",
            ],
        }
        roles = scryfall_search.classify_card_roles(mdfc_bounce)
        self.assertIn("removal", roles)
        self.assertNotIn("ramp", roles)

        deluge = {
            "type": "Sorcery",
            "oracles": ["As an additional cost to cast this spell, pay X life.\nAll creatures get -X/-X until end of turn."],
        }
        self.assertIn("board_wipe", scryfall_search.classify_card_roles(deluge))

    def test_scryfall_query_syntax_operators_and_safety(self):
        cards = [
            {
                "card_name": "Lightning Bolt",
                "type": "Instant",
                "oracles": ["Lightning Bolt deals 3 damage to any target."],
                "color_identity": ["R"],
                "cmc": 1.0,
                "roles": ["removal", "burn"],
            },
            {
                "card_name": "Counterspell",
                "type": "Instant",
                "oracles": ["Counter target spell."],
                "color_identity": ["U"],
                "cmc": 2.0,
                "roles": ["counterspell"],
            },
            {
                "card_name": "Kraum, Ludevic's Opus / Tymna the Weaver",
                "type": "Legendary Creature — Zombie Horror // Legendary Creature — Human Cleric",
                "oracles": ["Flying, haste", "Lifelink"],
                "color_identity": ["W", "U", "B", "R"],
                "cmc": 5.0,
                "roles": [],
            },
        ]

        # Boolean OR, type, cmc, negation, color identity, regex, role
        p1 = scryfall_search.compile_scryfall_query("(t:instant OR t:sorcery) cmc<=2 -c:u")
        self.assertEqual([c["card_name"] for c in cards if p1(c)], ["Lightning Bolt"])

        p2 = scryfall_search.compile_scryfall_query('o:/deals? \\d+ damage/ id:izzet')
        self.assertEqual([c["card_name"] for c in cards if p2(c)], ["Lightning Bolt"])

        p3 = scryfall_search.compile_scryfall_query('!"Counterspell"')
        self.assertEqual([c["card_name"] for c in cards if p3(c)], ["Counterspell"])

        p4 = scryfall_search.compile_scryfall_query("role:counterspell mv=even")
        self.assertEqual([c["card_name"] for c in cards if p4(c)], ["Counterspell"])

        p5 = scryfall_search.compile_scryfall_query("Kraum / Tymna")
        self.assertEqual(
            [c["card_name"] for c in cards if p5(c)],
            ["Kraum, Ludevic's Opus / Tymna the Weaver"],
        )

        # commander: / cmd: seguono le stesse regole di id: (color_identity <= target)
        p6 = scryfall_search.compile_scryfall_query("(commander:jund)")
        self.assertEqual([c["card_name"] for c in cards if p6(c)], ["Lightning Bolt"])

        p7 = scryfall_search.compile_scryfall_query("cmd:izzet -t:creature")
        self.assertEqual(
            [c["card_name"] for c in cards if p7(c)],
            ["Lightning Bolt", "Counterspell"],
        )

        # Verifica che (id:jund) non lasci passare carte con colori fuori B/R/G (es. W o U)
        p8 = scryfall_search.compile_scryfall_query("(id:jund)")
        self.assertEqual([c["card_name"] for c in cards if p8(c)], ["Lightning Bolt"])

        # Verifica che -is:fetchland escluda le fetchland incolori quando richiesto
        fetch_card = {
            "card_name": "Flooded Strand",
            "type": "Land",
            "oracles": ["{T}, Pay 1 life, Sacrifice this land: Search your library for a Plains or Island card, put it onto the battlefield, then shuffle."],
            "color_identity": [],
            "cmc": 0.0,
        }
        p9 = scryfall_search.compile_scryfall_query("commander:jund -is:fetchland")
        self.assertFalse(p9(fetch_card))
        self.assertTrue(p8(fetch_card))

        # Malformed / unclosed queries must never raise
        for bad_q in ["(((", 'o:"unclosed', "o:/[invalid/", "-", "t:"]:
            pred = scryfall_search.compile_scryfall_query(bad_q)
            self.assertIsInstance(pred(cards[0]), bool)

    def test_dashboard_role_categories_respect_period(self):
        from datetime import timedelta

        now = main.datetime.now()
        d_old = (now - timedelta(days=90)).strftime("%Y-%m-%dT12:00:00")
        d_new = (now - timedelta(days=2)).strftime("%Y-%m-%dT12:00:00")

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            main_db = os.path.join(tmpdir, "centurion.db")
            conn = sqlite3.connect(main_db)
            conn.execute("CREATE TABLE decks (id_moxfield TEXT PRIMARY KEY, comandante TEXT, data_aggiornamento TEXT)")
            conn.execute("CREATE TABLE deck_cards (deck_id TEXT, card_name TEXT)")
            conn.execute("INSERT INTO decks VALUES ('d1', 'Kraum', ?)", (d_new,))
            conn.execute("INSERT INTO decks VALUES ('d2', 'Kraum', ?)", (d_old,))
            conn.execute("INSERT INTO deck_cards VALUES ('d1', 'Lightning Bolt')")
            conn.execute("INSERT INTO deck_cards VALUES ('d2', 'Brainstorm')")
            conn.commit()
            conn.close()

            def _get_test_db():
                c = sqlite3.connect(main_db, timeout=30)
                c.row_factory = sqlite3.Row
                return c

            fake_scryfall = {
                "kraum": {"cmc": 5, "type": "Legendary Creature", "color_identity": ["U", "R"], "images": ["https://img/k.jpg"], "oracles": ["Flying"]},
                "lightning bolt": {"cmc": 1, "type": "Instant", "color_identity": ["R"], "images": ["https://img/lb.jpg"], "oracles": ["Lightning Bolt deals 3 damage to any target."]},
                "brainstorm": {"cmc": 1, "type": "Instant", "color_identity": ["U"], "images": ["https://img/bs.jpg"], "oracles": ["Draw three cards, then put two cards from your hand on top of your library in any order."]},
            }

            main.get_dashboard_data_cached.cache_clear()
            with (
                patch.object(main, "get_db", side_effect=_get_test_db),
                patch.object(main, "load_scryfall_cache", return_value=fake_scryfall),
                patch.object(main, "fetch_scryfall_data", return_value=fake_scryfall),
                patch.object(main, "get_global_commanders_validity", return_value=(frozenset({"Kraum"}), ())),
                patch.object(main, "get_recent_expansions_metadata", return_value=[]),
                patch.object(main, "get_db_mtime", return_value=1700000000.0),
            ):
                dash_30d = main.get_dashboard_data_cached("Ultimi 30 giorni")
                main.get_dashboard_data_cached.cache_clear()

            by_role = {r["key"]: r for r in dash_30d["role_categories"]}
            self.assertEqual([c["name"] for c in by_role["burn"]["cards"]], ["Lightning Bolt"])
            # Brainstorm è solo nei 90 giorni fa, non negli ultimi 30 giorni
            self.assertEqual(by_role["card_draw"]["cards"], [])


if __name__ == "__main__":
    unittest.main()
