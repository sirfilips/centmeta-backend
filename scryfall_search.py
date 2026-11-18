import re
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ==============================================================================
# CLASSIFICAZIONE RUOLI FUNZIONALI (HOME DASHBOARD & FILTRI)
# ==============================================================================

ROLE_DEFINITIONS: List[Dict[str, str]] = [
    {
        "key": "removal",
        "label": "Removal",
        "title": "Migliori Removal del Formato",
        "description": "Rimozioni mirate (distruzione, esilio, danni a creature/planeswalker, -X/-X, editti e rimbalzi) ordinate per numero di mazzi nel periodo selezionato.",
        "accent": "rose",
    },
    {
        "key": "burn",
        "label": "Botti",
        "title": "Migliori Botti del Formato",
        "description": "Carte che infliggono danni diretti a qualsiasi bersaglio o ai giocatori/avversari nel periodo selezionato.",
        "accent": "orange",
    },
    {
        "key": "counterspell",
        "label": "Counterspell",
        "title": "Migliori Counterspell del Formato",
        "description": "Magie e risposte che neutralizzano magie o abilità avversarie (inclusi bounce sullo stack) nel periodo selezionato.",
        "accent": "blue",
    },
    {
        "key": "card_draw",
        "label": "Card Draw",
        "title": "Migliori Card Draw del Formato",
        "description": "Motori di pescata, peschini e cantrip puri per generare vantaggio carte e selezione nel periodo selezionato.",
        "accent": "cyan",
    },
    {
        "key": "discard",
        "label": "Scartini",
        "title": "Migliori Scartini del Formato",
        "description": "Magie e carte di disruption che rivelano o fanno scartare carte dalla mano avversaria nel periodo selezionato.",
        "accent": "violet",
    },
    {
        "key": "board_wipe",
        "label": "Board Wipe",
        "title": "Migliori Board Wipe del Formato",
        "description": "Rimozioni di massa e sweeper per ripulire il tavolo da creature, artefatti o permanenti nel periodo selezionato.",
        "accent": "amber",
    },
    {
        "key": "tutor",
        "label": "Tutori",
        "title": "Migliori Tutori del Formato",
        "description": "Carte che cercano pezzi specifici dal grimorio (escluse le semplici fetch/ricerche di terre) nel periodo selezionato.",
        "accent": "emerald",
    },
    {
        "key": "ramp",
        "label": "Ramp",
        "title": "Migliori Ramp del Formato",
        "description": "Mana dork, mana rock, acceleratori di terre e rituali per generare mana extra nel periodo selezionato.",
        "accent": "lime",
    },
]

ROLE_ALIASES: Dict[str, str] = {
    "removal": "removal",
    "removals": "removal",
    "rimozione": "removal",
    "rimozioni": "removal",
    "burn": "burn",
    "botti": "burn",
    "botto": "burn",
    "spari": "burn",
    "counter": "counterspell",
    "counters": "counterspell",
    "counterspell": "counterspell",
    "counterspells": "counterspell",
    "draw": "card_draw",
    "card_draw": "card_draw",
    "carddraw": "card_draw",
    "peschini": "card_draw",
    "discard": "discard",
    "scartini": "discard",
    "scartino": "discard",
    "hand_disruption": "discard",
    "wipe": "board_wipe",
    "wipes": "board_wipe",
    "board_wipe": "board_wipe",
    "boardwipe": "board_wipe",
    "sweeper": "board_wipe",
    "sweepers": "board_wipe",
    "tutor": "tutor",
    "tutors": "tutor",
    "tutori": "tutor",
    "tutore": "tutor",
    "ramp": "ramp",
    "mana": "ramp",
}

_NON_LAND_TYPES = ("creature", "artifact", "enchantment", "instant", "sorcery", "planeswalker", "battle")
_REMINDER_RE = re.compile(r"\([^)]*\)")


def _extract_nonland_oracle_and_flags(card_info: dict) -> Tuple[str, str, bool, bool, bool]:
    """Estrae testo oracle pulito (escludendo il retro terra delle MDFC e i reminder text)."""
    type_line = (card_info.get("type") or "").lower()
    oracles_list = card_info.get("oracles") or []
    type_parts = [p.strip() for p in type_line.split(" // ") if p.strip()]

    nonland_oracles: List[str] = []
    if len(oracles_list) > 1 and len(type_parts) == len(oracles_list):
        for tp, orc in zip(type_parts, oracles_list):
            if "land" not in tp or any(nt in tp for nt in _NON_LAND_TYPES):
                nonland_oracles.append(orc)
    else:
        nonland_oracles = list(oracles_list)

    front_type = type_parts[0] if type_parts else ""
    is_pure_land = "land" in front_type and not any(nt in front_type for nt in _NON_LAND_TYPES)
    has_land_front = "land" in front_type
    is_instant_sorcery = "instant" in type_line or "sorcery" in type_line

    oracle_raw = " ".join(nonland_oracles)
    oracle_clean = _REMINDER_RE.sub("", oracle_raw).lower()
    return type_line, oracle_clean, is_pure_land, has_land_front, is_instant_sorcery


def classify_card_roles(card_info: dict) -> List[str]:
    """Classifica una carta nei ruoli funzionali Centurion sulla base di oracle text e type line."""
    if not card_info:
        return []

    type_line, o, is_pure_land, has_land_front, is_instant_sorcery = _extract_nonland_oracle_and_flags(card_info)
    if not o and not type_line:
        return []

    roles: Set[str] = set()

    # 1. Counterspell
    if (
        re.search(r"\bcounter target\b[^.]*\b(spell|activated|triggered|ability)\b", o)
        or re.search(r"\breturn target spell to its owner's hand\b", o)
    ):
        roles.add("counterspell")

    # 2. Discard / Scartini
    if (
        re.search(r"\b(target (?:player|opponent)|each opponent) (?:reveals their hand|discards?)\b", o)
        or re.search(r"\btarget opponent discards\b", o)
        or re.search(r"\bchoose a [^\n.]*card from (?:it|that player's hand)\b[^\n.]*\bdiscard", o)
    ):
        roles.add("discard")

    # 3. Board Wipe / Sweeper
    if not is_pure_land and (
        re.search(
            r"\b(?:destroy|exile) all (?:creatures|nonland permanents|artifacts|enchantments|permanents|artifacts and creatures|artifacts and enchantments)\b",
            o,
        )
        or re.search(r"\ball creatures get -[0-9x]+/-[0-9x]+\b", o)
        or re.search(r"\bdeals? (?:[0-9]+|x) damage to each (?:creature|non[a-z]+ creature)\b", o)
        or re.search(r"\breturn all (?:nonland permanents|creatures) [^\n.]*to their owners' hands\b", o)
    ):
        roles.add("board_wipe")

    # 4. Burn / Botti (danni diretti a giocatore / avversario / qualsiasi bersaglio)
    if not is_pure_land and (
        re.search(
            r"\bdeals? (?:[0-9]+|x) damage to (?:any target|target player|target opponent|each opponent|target creature or player|up to [a-z]+ targets)\b",
            o,
        )
        or re.search(
            r"\bdeals? damage equal to [^\n.]* to (?:any target|target player|target opponent|each opponent)\b",
            o,
        )
    ):
        roles.add("burn")

    # 5. Removal (consente utility channel land come Boseiju/Otawara/Eiganjo, esclude LD puro come Wasteland)
    has_targeted_removal = bool(
        re.search(
            r"\b(?:destroy|exile) (?:up to [a-z]+ )?target (?:creature|artifact|enchantment|planeswalker|permanent|nonland permanent|attacking|blocking|tapped)\b",
            o,
        )
        or re.search(
            r"\b(?:destroy|exile) target [a-z, ]*(?:or )?(?:creature|artifact|enchantment|planeswalker|nonland permanent)\b",
            o,
        )
        or re.search(r"\btarget (?:creature|planeswalker) gets -[0-9x]+/-[0-9x]+\b", o)
        or re.search(r"\btarget (?:player|opponent) sacrifices a (?:creature|nonland permanent|permanent)\b", o)
        or re.search(
            r"\bdeals? (?:[0-9]+|x) damage (?:divided as you choose )?to (?:up to [a-z]+ )?(?:any target|target creature|target planeswalker|target creature or planeswalker)\b",
            o,
        )
        or re.search(
            r"\breturn (?:up to [a-z]+ )?target [a-z, ]*(?:nonland permanent|creature|artifact|enchantment|planeswalker) [^\n.]*to its owner's hand\b",
            o,
        )
        or ("aura" in type_line and re.search(r"\benchanted creature (?:can't attack|loses all abilities)\b", o))
    )
    if has_targeted_removal and not (
        "target nonbasic land" in o and "creature" not in o and "nonland" not in o and "artifact" not in o
    ):
        roles.add("removal")

    # 6. Card Draw (esclude trigger punitivi "draws a card" / "whenever you draw a card" e cantrip su removal/counter)
    has_multi_draw = bool(re.search(r"\bdraw (?:two|three|four|five|six|seven|x) cards?\b", o))
    has_single_draw = bool(re.search(r"(?<!whenever you )(?<!whenever a player )(?<!opponent )(?<!opponents )\bdraw a card\b", o))
    has_card_selection_hand = bool(
        re.search(r"\b(?:look at the top|reveal the top) [^\n.]*put [^\n.]* into your hand\b", o)
        or re.search(r"\bput [^\n.]* from among them into your hand\b", o)
    )
    if not is_pure_land and (has_multi_draw or has_single_draw or has_card_selection_hand):
        is_cantrip_rider = (has_single_draw and not has_multi_draw) and bool(
            roles & {"counterspell", "removal", "discard", "board_wipe"}
        )
        if not is_cantrip_rider:
            roles.add("card_draw")

    # 7. Tutor (esclude terre come Urza's Saga e ricerche di sole terre)
    if not has_land_front and re.search(
        r"\bsearch your library for (?:a|an|up to [a-z]+) (?!basic land|land card|plains|island|swamp|mountain|forest)",
        o,
    ):
        if not re.search(
            r"\bsearch your library for (?:a|an|up to [a-z]+) (?:basic )?(?:plains|island|swamp|mountain|forest|land)(?:,| or| card)",
            o,
        ):
            roles.add("tutor")

    # 8. Ramp (dork, mana rock, rituali, ramp di terre sul campo)
    if not has_land_front and (
        re.search(r"\{t\}(?:, [^:]*)?:\s*add \{", o)
        or re.search(r"\{t\}(?:, [^:]*)?:\s*add one mana\b", o)
        or (
            re.search(r"\bsearch your library for (?:a|an|up to [a-z]+) (?:basic )?(?:plains|island|swamp|mountain|forest|land)", o)
            and "onto the battlefield" in o
            and "sacrifice a land" not in o
        )
        or (is_instant_sorcery and re.search(r"\badd \{[wubrgc]+\}", o))
        or ("land" in o and "additional land" in o)
    ):
        roles.add("ramp")

    return [r["key"] for r in ROLE_DEFINITIONS if r["key"] in roles]


# ==============================================================================
# MOTORE DI RICERCA SINTASSI SCRYFALL
# ==============================================================================

COLOR_WORDS: Dict[str, Set[str]] = {
    "w": {"W"},
    "white": {"W"},
    "u": {"U"},
    "blue": {"U"},
    "b": {"B"},
    "black": {"B"},
    "r": {"R"},
    "red": {"R"},
    "g": {"G"},
    "green": {"G"},
    "azorius": {"W", "U"},
    "dimir": {"U", "B"},
    "rakdos": {"B", "R"},
    "gruul": {"R", "G"},
    "selesnya": {"G", "W"},
    "orzhov": {"W", "B"},
    "izzet": {"U", "R"},
    "golgari": {"B", "G"},
    "boros": {"R", "W"},
    "simic": {"G", "U"},
    "bant": {"W", "U", "G"},
    "esper": {"W", "U", "B"},
    "grixis": {"U", "B", "R"},
    "jund": {"B", "R", "G"},
    "naya": {"R", "G", "W"},
    "abzan": {"W", "B", "G"},
    "jeskai": {"U", "R", "W"},
    "sultai": {"B", "G", "U"},
    "mardu": {"R", "W", "B"},
    "temur": {"G", "U", "R"},
    "wubrg": {"W", "U", "B", "R", "G"},
}

RARITY_ALIASES: Dict[str, str] = {
    "c": "common",
    "common": "common",
    "u": "uncommon",
    "uncommon": "uncommon",
    "r": "rare",
    "rare": "rare",
    "m": "mythic",
    "mythic": "mythic",
}

_KNOWN_FIELDS = {
    "o", "oracle", "fo", "fulloracle",
    "t", "type",
    "c", "color", "colors",
    "id", "identity", "ci", "commander", "cmd",
    "mv", "cmc", "manavalue",
    "m", "mana",
    "kw", "keyword",
    "pow", "power",
    "tou", "toughness",
    "loy", "loyalty",
    "e", "set", "s",
    "r", "rarity",
    "n", "name",
    "is", "not",
    "role", "function", "otag",
    "decks", "freq", "perc", "sinergia",
}

_STAT_FIELD_MAP = {
    "pow": "power",
    "power": "power",
    "tou": "toughness",
    "toughness": "toughness",
    "loy": "loyalty",
    "loyalty": "loyalty",
}

_VALID_COLOR_SET = {"W", "U", "B", "R", "G"}

_FIELD_OP_RE = re.compile(r"^([a-zA-Z]+)(!=|<=|>=|:|=|<|>)(.*)$", re.DOTALL)


def _tokenize_scryfall_query(query: str) -> List[Tuple[str, str]]:
    """Tokenizza una query Scryfall gestendo virgolette, regex /.../, parentesi e negazione '-'."""
    tokens: List[Tuple[str, str]] = []
    i = 0
    n = len(query)

    while i < n:
        ch = query[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "(":
            tokens.append(("LPAREN", "("))
            i += 1
            continue
        if ch == ")":
            tokens.append(("RPAREN", ")"))
            i += 1
            continue

        # Leggi un token completo (rispettando "..." e /.../)
        buf: List[str] = []
        while i < n:
            c = query[i]
            if c.isspace() or c in ("(", ")"):
                if not buf and c == "(":
                    break
                if buf:
                    break
            if c == '"':
                buf.append(c)
                i += 1
                while i < n:
                    if query[i] == "\\" and i + 1 < n:
                        buf.append(query[i + 1])
                        i += 2
                        continue
                    if query[i] == '"':
                        buf.append('"')
                        i += 1
                        break
                    buf.append(query[i])
                    i += 1
                continue
            if c == "/" and (
                (buf and buf[-1] in (":", "=", "<", ">"))
                or (not buf and i + 1 < n and query[i + 1] not in ("/", " ") and "/" in query[i + 1 :])
            ):
                buf.append(c)
                i += 1
                while i < n:
                    if query[i] == "\\" and i + 1 < n:
                        buf.append(query[i])
                        buf.append(query[i + 1])
                        i += 2
                        continue
                    if query[i] == "/":
                        buf.append("/")
                        i += 1
                        break
                    buf.append(query[i])
                    i += 1
                continue
            buf.append(c)
            i += 1

        word = "".join(buf)
        if not word:
            continue
        upper = word.upper()
        if upper == "OR":
            tokens.append(("OR", "OR"))
        elif upper == "AND":
            tokens.append(("AND", "AND"))
        else:
            tokens.append(("TERM", word))

    return tokens


def _strip_quotes(val: str) -> str:
    if len(val) >= 2 and val[0] == '"' and val[-1] == '"':
        return val[1:-1]
    return val


def _parse_regex_literal(val: str) -> Optional[re.Pattern]:
    if len(val) >= 2 and val[0] == "/" and val[-1] == "/":
        pattern_str = val[1:-1]
        try:
            return re.compile(pattern_str, re.IGNORECASE)
        except re.error:
            return None
    return None


def _parse_color_expr(val: str) -> Tuple[Optional[int], Optional[str], Set[str]]:
    """
    Analizza un valore colore/identità:
    Restituisce (numeric_count, special_mode ['c'|'m'|None], set_di_colori).
    """
    v = _strip_quotes(val).strip().lower()
    if not v:
        return None, None, set()
    if v.isdigit():
        return int(v), None, set()
    if v in ("c", "colorless"):
        return None, "c", set()
    if v in ("m", "multicolor"):
        return None, "m", set()
    if v in COLOR_WORDS:
        return None, None, set(COLOR_WORDS[v])
    letters = set()
    for ch in v:
        if ch in ("w", "u", "b", "r", "g"):
            letters.add(ch.upper())
        else:
            # Carattere non valido: restituisce un set non vuoto sentinella? No, fallback lettera
            return None, None, set()
    return None, None, letters


def _compile_numeric_comparator(op: str, target_str: str) -> Callable[[Optional[float]], bool]:
    """Pre-compila il confronto numerico evitando parsing ripetuto per ogni carta."""
    t = _strip_quotes(target_str).strip().lower()
    if t == "even":
        return lambda actual: actual is not None and float(actual).is_integer() and int(actual) % 2 == 0
    if t == "odd":
        return lambda actual: actual is not None and float(actual).is_integer() and int(actual) % 2 == 1
    try:
        target = float(t)
    except ValueError:
        return lambda _actual: False

    if op in (":", "="):
        return lambda actual: actual is not None and actual == target
    if op == "!=":
        return lambda actual: actual is not None and actual != target
    if op == "<":
        return lambda actual: actual is not None and actual < target
    if op == "<=":
        return lambda actual: actual is not None and actual <= target
    if op == ">":
        return lambda actual: actual is not None and actual > target
    if op == ">=":
        return lambda actual: actual is not None and actual >= target
    return lambda _actual: False


def _compile_color_comparator(op: str, val: str, is_identity: bool) -> Callable[[Optional[List[str]]], bool]:
    """Pre-compila il confronto colore/identità per valutare migliaia di carte a costo minimo."""
    num_count, special, target = _parse_color_expr(val)

    if num_count is not None:
        num_cmp = _compile_numeric_comparator(op, str(num_count))
        return lambda actual_list: num_cmp(
            float(sum(1 for c in (actual_list or []) if c and c.upper() in _VALID_COLOR_SET))
        )

    if special == "c":
        want_not = op == "!="
        def _c_pred(actual_list: Optional[List[str]]) -> bool:
            has_color = any(c and c.upper() in _VALID_COLOR_SET for c in (actual_list or []))
            return has_color if want_not else (not has_color)
        return _c_pred

    if special == "m":
        want_not = op == "!="
        def _m_pred(actual_list: Optional[List[str]]) -> bool:
            cnt = len({c.upper() for c in (actual_list or []) if c and c.upper() in _VALID_COLOR_SET})
            is_m = cnt > 1
            return (not is_m) if want_not else is_m
        return _m_pred

    if not target:
        return lambda _actual_list: False

    def _set_pred(actual_list: Optional[List[str]]) -> bool:
        actual = {c.upper() for c in (actual_list or []) if c and c.upper() in _VALID_COLOR_SET}
        if op == "=":
            return actual == target
        if op == "!=":
            return actual != target
        if op == "<=":
            return actual.issubset(target)
        if op == "<":
            return actual.issubset(target) and actual != target
        if op == ">=":
            return target.issubset(actual)
        if op == ">":
            return target.issubset(actual) and actual != target
        # Operatore ':' -> id:/commander: significa <= (rientra nell'identità), c: significa >=
        return actual.issubset(target) if is_identity else target.issubset(actual)

    return _set_pred


def _compile_text_matcher(raw_val: str, exact: bool = False) -> Callable[[str], bool]:
    """Pre-compila regex o sottostringa lowercase per ricerche testuali rapide."""
    regex = _parse_regex_literal(raw_val)
    if regex is not None:
        return lambda text: bool(regex.search(text))
    needle = _strip_quotes(raw_val).strip().lower()
    if not needle:
        return lambda _text: True
    if exact:
        return lambda text: text.lower() == needle
    return lambda text: needle in text.lower()


def _normalize_mana_query(raw_val: str) -> str:
    v = _strip_quotes(raw_val).strip().upper()
    if "{" in v:
        return v
    return "".join(f"{{{ch}}}" for ch in v if not ch.isspace())


def _match_is_property(card: Dict[str, Any], prop: str) -> bool:
    p = _strip_quotes(prop).strip().lower()
    name = (card.get("name") or card.get("card_name") or card.get("comandante") or "").lower()
    type_line = (card.get("type") or "").lower()
    oracles = " ".join(card.get("oracles") or []).lower()
    ci = card.get("color_identity") or []
    colors = card.get("colors")
    if colors is None:
        colors = [] if "land" in type_line.split(" // ")[0] else ci
    mana_cost = (card.get("mana_cost") or "").lower()

    if p in ("creature", "instant", "sorcery", "artifact", "enchantment", "planeswalker", "land", "battle", "legendary", "basic", "snow"):
        return p in type_line
    if p in ("tribal", "kindred"):
        return "tribal" in type_line or "kindred" in type_line
    if p == "nonland":
        return "land" not in type_line.split(" // ")[0]
    if p == "permanent":
        return any(t in type_line for t in ("creature", "artifact", "enchantment", "planeswalker", "land", "battle"))
    if p == "spell":
        return "land" not in type_line.split(" // ")[0]
    if p == "historic":
        return "artifact" in type_line or "legendary" in type_line or "saga" in type_line
    if p in ("commander", "cmd"):
        return ("legendary" in type_line and "creature" in type_line) or "can be your commander" in oracles
    if p == "partner":
        return " / " in name or "partner" in oracles or "friends forever" in oracles or "choose a background" in oracles
    if p in ("dfc", "transform", "mdfc", "split"):
        return " // " in name or " // " in type_line
    if p == "modal":
        return any(k in oracles for k in ("choose one", "choose two", "choose three", "escalate", "spree")) or " // " in name
    if p == "vanilla":
        return "creature" in type_line and not oracles.strip()
    if p == "multicolor":
        return len(colors) > 1
    if p == "monocolor":
        return len(colors) == 1
    if p == "colorless":
        return len(colors) == 0
    if p == "hybrid":
        return bool(re.search(r"\{[wubrg]/[wubrg2]\}", mana_cost or oracles))
    if p == "phyrexian":
        return bool(re.search(r"\{[wubrg]/p\}", mana_cost or oracles))
    if p == "etb":
        return "enters" in oracles or "enters the battlefield" in oracles
    if p == "fetchland":
        return "land" in type_line and "sacrifice this land: search your library for" in oracles
    if p == "shockland":
        return "land" in type_line and "you may pay 2 life" in oracles
    if p == "dual":
        return "land" in type_line and not oracles.strip() and "basic" not in type_line
    if p == "painland":
        return "land" in type_line and "deals 1 damage to you" in oracles
    if p == "surveilland":
        return "land" in type_line and "surveil 1" in oracles
    if p in ROLE_ALIASES:
        canonical = ROLE_ALIASES[p]
        roles = card.get("roles")
        if roles is None:
            roles = classify_card_roles(card)
        return canonical in roles
    return False


def _get_item_name(card: Dict[str, Any]) -> str:
    return card.get("name") or card.get("card_name") or card.get("comandante") or ""


def _build_term_predicate(term: str) -> Callable[[Dict[str, Any]], bool]:
    # Gestione prefisso esatto '!'
    if term.startswith("!"):
        target = _strip_quotes(term[1:]).strip().lower()
        def _exact_pred(card: Dict[str, Any]) -> bool:
            name = _get_item_name(card).strip().lower()
            if name == target:
                return True
            if " // " in name:
                return any(p.strip() == target for p in name.split(" // "))
            return False
        return _exact_pred

    m = _FIELD_OP_RE.match(term)
    if m:
        field = m.group(1).lower()
        op = m.group(2)
        val = m.group(3)
        if field in _KNOWN_FIELDS:
            if field in ("o", "oracle", "fo", "fulloracle"):
                base_matcher = _compile_text_matcher(val)
                has_tilde = "~" in val
                def _oracle_pred(card: Dict[str, Any]) -> bool:
                    oracles_str = "\n".join(card.get("oracles") or [])
                    if not oracles_str:
                        return False
                    if base_matcher(oracles_str):
                        return True
                    if has_tilde:
                        name = _get_item_name(card)
                        if name:
                            return _compile_text_matcher(val.replace("~", name))(oracles_str)
                    return False
                return _oracle_pred

            if field in ("t", "type"):
                type_matcher = _compile_text_matcher(val)
                return lambda card: type_matcher(card.get("type") or "")

            if field in ("n", "name"):
                name_matcher = _compile_text_matcher(val, exact=(op == "="))
                return lambda card: name_matcher(_get_item_name(card))

            if field in ("id", "identity", "ci", "commander", "cmd"):
                id_cmp = _compile_color_comparator(op, val, is_identity=True)
                return lambda card: id_cmp(card.get("color_identity") or [])

            if field in ("c", "color", "colors"):
                color_cmp = _compile_color_comparator(op, val, is_identity=False)
                def _color_pred(card: Dict[str, Any]) -> bool:
                    colors = card.get("colors")
                    if colors is None:
                        t_front = (card.get("type") or "").lower().split(" // ")[0]
                        colors = [] if "land" in t_front else (card.get("color_identity") or [])
                    return color_cmp(colors)
                return _color_pred

            if field in ("mv", "cmc", "manavalue"):
                cmc_cmp = _compile_numeric_comparator(op, val)
                def _cmc_pred(card: Dict[str, Any]) -> bool:
                    raw = card.get("cmc")
                    return False if raw is None else cmc_cmp(float(raw))
                return _cmc_pred

            if field in _STAT_FIELD_MAP:
                key = _STAT_FIELD_MAP[field]
                stat_cmp = _compile_numeric_comparator(op, val)
                val_lower = _strip_quotes(val).strip().lower()
                def _stat_pred(card: Dict[str, Any]) -> bool:
                    raw = card.get(key)
                    if raw is None or raw == "":
                        return False
                    try:
                        return stat_cmp(float(raw))
                    except (TypeError, ValueError):
                        return val_lower == str(raw).strip().lower()
                return _stat_pred

            if field in ("m", "mana"):
                norm_q = _normalize_mana_query(val)
                def _mana_pred(card: Dict[str, Any]) -> bool:
                    mc = (card.get("mana_cost") or "").upper()
                    if not mc:
                        return False
                    return mc == norm_q if op == "=" else norm_q in mc
                return _mana_pred

            if field in ("kw", "keyword"):
                kw_needle = _strip_quotes(val).strip().lower()
                kw_regex = re.compile(rf"(?:^|[\n,])\s*{re.escape(kw_needle)}\b", re.IGNORECASE) if kw_needle else None
                def _kw_pred(card: Dict[str, Any]) -> bool:
                    if not kw_needle:
                        return True
                    kws = card.get("keywords") or []
                    if any(kw_needle in k.lower() for k in kws):
                        return True
                    if not kw_regex:
                        return False
                    return bool(kw_regex.search("\n".join(card.get("oracles") or [])))
                return _kw_pred

            if field in ("e", "set", "s"):
                s_needle = _strip_quotes(val).strip().lower()
                def _set_pred(card: Dict[str, Any]) -> bool:
                    sc = (card.get("set_code") or card.get("set") or "").strip().lower()
                    return sc == s_needle if op == "=" else (s_needle in sc if sc else False)
                return _set_pred

            if field in ("r", "rarity"):
                raw_r = _strip_quotes(val).strip().lower()
                r_target = RARITY_ALIASES.get(raw_r, raw_r)
                return lambda card: (card.get("rarity") or "").strip().lower() == r_target

            if field in ("is", "not"):
                prop_clean = _strip_quotes(val).strip().lower()
                negate = field == "not"
                return (lambda card: not _match_is_property(card, prop_clean)) if negate else (lambda card: _match_is_property(card, prop_clean))

            if field in ("role", "function", "otag"):
                canonical = ROLE_ALIASES.get(_strip_quotes(val).strip().lower())
                def _role_pred(card: Dict[str, Any]) -> bool:
                    if not canonical:
                        return False
                    roles = card.get("roles")
                    if roles is None:
                        roles = classify_card_roles(card)
                    return canonical in roles
                return _role_pred

            if field in ("decks", "freq", "perc", "sinergia"):
                metric_cmp = _compile_numeric_comparator(op, val)
                def _metric_pred(card: Dict[str, Any]) -> bool:
                    if field == "perc":
                        raw = card.get("Percentuale")
                    elif field == "sinergia":
                        raw = card.get("Sinergia")
                    else:
                        raw = card.get("freq") or card.get("cnt") or card.get("val1")
                    return False if raw is None else metric_cmp(float(raw))
                return _metric_pred

    # Ricerca testuale semplice sul nome della carta/comandante
    plain_matcher = _compile_text_matcher(term)
    return lambda card: plain_matcher(_get_item_name(card))


def compile_scryfall_query(query: str) -> Callable[[Dict[str, Any]], bool]:
    """
    Compila una query con sintassi Scryfall in un predicato riutilizzabile
    card_dict -> bool. Sicuro contro sintassi incompleta o regex non valide.
    """
    q = (query or "").strip()
    if not q:
        return lambda _card: True

    tokens = _tokenize_scryfall_query(q)
    if not tokens:
        return lambda _card: True

    pos = 0
    total = len(tokens)

    def peek() -> Optional[Tuple[str, str]]:
        return tokens[pos] if pos < total else None

    def consume() -> Tuple[str, str]:
        nonlocal pos
        tok = tokens[pos]
        pos += 1
        return tok

    def parse_or() -> Callable[[Dict[str, Any]], bool]:
        left = parse_and()
        while peek() and peek()[0] == "OR":
            consume()
            right = parse_and()
            l_fn, r_fn = left, right
            left = lambda c, a=l_fn, b=r_fn: a(c) or b(c)
        return left

    def parse_and() -> Callable[[Dict[str, Any]], bool]:
        terms: List[Callable[[Dict[str, Any]], bool]] = []
        while True:
            nxt = peek()
            if not nxt or nxt[0] in ("RPAREN", "OR"):
                break
            if nxt[0] == "AND":
                consume()
                continue
            terms.append(parse_unary())

        if not terms:
            return lambda _c: True
        if len(terms) == 1:
            return terms[0]

        def _all(c: Dict[str, Any], preds=terms) -> bool:
            return all(fn(c) for fn in preds)

        return _all

    def parse_unary() -> Callable[[Dict[str, Any]], bool]:
        nxt = peek()
        if not nxt:
            return lambda _c: True

        if nxt[0] == "LPAREN":
            consume()
            inner = parse_or()
            if peek() and peek()[0] == "RPAREN":
                consume()
            return inner

        if nxt[0] == "TERM":
            val = consume()[1]
            if val == "-":
                sub = parse_unary()
                return lambda c, s=sub: not s(c)
            if val.startswith("-") and len(val) > 1:
                inner = _build_term_predicate(val[1:])
                return lambda c, fn=inner: not fn(c)
            return _build_term_predicate(val)

        # Fallback per eventuali token inattesi
        consume()
        return lambda _c: True

    return parse_or()
