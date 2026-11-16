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


class TestCommunityVotesConcurrency(unittest.TestCase):
    def test_concurrent_votes_accumulate_accurately(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
            comm_db = os.path.join(tmpdir, "community.db")

            def _get_test_comm_db():
                conn = sqlite3.connect(comm_db, timeout=30, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout = 30000;")
                return conn

            with (
                patch.object(main, "get_community_db", side_effect=_get_test_comm_db),
                patch.object(main, "get_community_card_pools", return_value=(["Lightning Bolt"], [], [])),
                patch.object(main, "_COMMUNITY_VALID_CARDS_SET", {"Lightning Bolt"}),
                patch.object(main, "is_land_card", return_value=False),
            ):
                main.setup_community_db()

                errors = []
                barrier = threading.Barrier(8)

                def _worker(idx: int):
                    try:
                        barrier.wait(timeout=5)
                        main.record_community_vote("Lightning Bolt", 5 if idx % 2 == 0 else 3, f"voter_{idx}")
                    except Exception as exc:
                        errors.append(exc)

                threads = [threading.Thread(target=_worker, args=(i,)) for i in range(8)]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join(timeout=10)

                self.assertEqual(errors, [])
                conn = _get_test_comm_db()
                try:
                    row = conn.execute(
                        "SELECT total_votes, score_sum, avg_score, stars_5, stars_3 FROM community_ratings WHERE card_name = ?",
                        ("Lightning Bolt",),
                    ).fetchone()
                finally:
                    conn.close()

                self.assertIsNotNone(row)
                self.assertEqual(row["total_votes"], 8)
                self.assertEqual(row["score_sum"], 32)
                self.assertEqual(row["avg_score"], 4.0)
                self.assertEqual(row["stars_5"], 4)
                self.assertEqual(row["stars_3"], 4)


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

        # Malformed / unclosed queries must never raise
        for bad_q in ["(((", 'o:"unclosed', "o:/[invalid/", "-", "t:"]:
            pred = scryfall_search.compile_scryfall_query(bad_q)
            self.assertIsInstance(pred(cards[0]), bool)

    def test_dashboard_role_categories_respect_period_and_leaderboard_scryfall(self):
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

            comm_db = os.path.join(tmpdir, "community.db")

            def _get_test_db():
                c = sqlite3.connect(main_db, timeout=30)
                c.row_factory = sqlite3.Row
                return c

            def _get_test_comm_db():
                c = sqlite3.connect(comm_db, timeout=30)
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

            with (
                patch.object(main, "get_community_db", side_effect=_get_test_comm_db),
                patch.object(main, "load_scryfall_cache", return_value=fake_scryfall),
                patch.object(main, "fetch_scryfall_data", return_value=fake_scryfall),
            ):
                main.setup_community_db()
                c = _get_test_comm_db()
                c.execute("INSERT INTO community_ratings (card_name, total_votes, score_sum, avg_score) VALUES ('Lightning Bolt', 5, 25, 5.0)")
                c.execute("INSERT INTO community_ratings (card_name, total_votes, score_sum, avg_score) VALUES ('Brainstorm', 5, 24, 4.8)")
                c.commit()
                c.close()

                lb = main.get_community_leaderboard(min_votes=2, search="o:damage id:r")
                self.assertEqual(lb["total"], 1)
                self.assertEqual(lb["items"][0]["card_name"], "Lightning Bolt")


if __name__ == "__main__":
    unittest.main()
