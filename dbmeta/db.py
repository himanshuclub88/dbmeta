import os, json, re, math
from collections import defaultdict, namedtuple

# ------------------------------
# TableQuery: python query API
# ------------------------------
class TableQuery:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows
    
    def show(self):
        print()
        rows=self.rows
        if not rows:
            print("(empty)")
            return

        # extract columns
        cols = sorted({k for row in rows for k in row.keys()})

        # compute column widths
        widths = {col: max(len(col), max(len(str(row.get(col, ""))) for row in rows))
                for col in cols}

        # print header
        header = " | ".join(col.ljust(widths[col]) for col in cols)
        print(header)
        print("-" * len(header))

        # print rows
        for row in rows:
            line = " | ".join(str(row.get(col, "")).ljust(widths[col]) for col in cols)
            print(line)


    def where(self, *conditions):
        """
        conditions: tuples like ("col", "=", value), ("col","contains","text")
        This is convenience - for advanced SQL use the SQL parser.
        """
        def check(row):
            for col, op, val in conditions:
                v = row.get(col)
                if op == "=":
                    if v != val: return False
                elif op == "!=":
                    if v == val: return False
                elif op == ">":
                    if not (isinstance(v, (int,float)) and v > val): return False
                elif op == "<":
                    if not (isinstance(v, (int,float)) and v < val): return False
                elif op == ">=":
                    if not (isinstance(v, (int,float)) and v >= val): return False
                elif op == "<=":
                    if not (isinstance(v, (int,float)) and v <= val): return False
                elif op == "contains":
                    if not (isinstance(v, str) and val.lower() in v.lower()): return False
                elif op == "in":
                    if v not in val: return False
                elif op == "not in":
                    if v in val: return False
                else:
                    return False
            return True

        return TableQuery([r for r in self.rows if check(r)])

    def select(self, *cols):
        return TableQuery([{c: r.get(c) for c in cols} for r in self.rows])

    def order_by(self, col, desc=False):
        try:
            return TableQuery(sorted(self.rows, key=lambda r: r.get(col), reverse=desc))
        except Exception:
            return self

    def limit(self, n):
        return TableQuery(self.rows[:n])

    def join(self, other, on):
        res = []
        for l in self.rows:
            lv = l.get(on)
            for r in other.rows:
                if r.get(on) == lv:
                    merged = dict(l)
                    # if same key, keep left's key, right's keys prefixed? We'll keep right overriding for now
                    merged.update(r)
                    res.append(merged)
        return TableQuery(res)

    def multi_join(self, tables, on):
        joined = TableQuery(self.rows)
        for t in tables:
            joined = joined.join(t, on)
        return joined

    def group_by(self, *cols):
        groups = defaultdict(list)
        for r in self.rows:
            key = tuple(r.get(c) for c in cols)
            groups[key].append(r)

        grouped_rows = []
        for key, rows in groups.items():
            group = {"_group_key": key, "_rows": rows, "COUNT": len(rows)}
            # auto numeric aggregates
            numeric_cols = {k for r in rows for k,v in r.items() if isinstance(v, (int,float))}
            for col in numeric_cols:
                vals = [r[col] for r in rows if isinstance(r.get(col), (int,float))]
                if vals:
                    group[f"SUM_{col}"] = sum(vals)
                    group[f"MIN_{col}"] = min(vals)
                    group[f"MAX_{col}"] = max(vals)
                    group[f"AVG_{col}"] = sum(vals) / len(vals)
            grouped_rows.append(group)
        return TableQuery(grouped_rows)

    def having(self, *conditions):
        def check(row):
            for col, op, val in conditions:
                v = row.get(col)
                if op == "=":
                    if v != val: return False
                elif op == "!=":
                    if v == val: return False
                elif op == ">":
                    if not (isinstance(v, (int,float)) and v > val): return False
                elif op == "<":
                    if not (isinstance(v, (int,float)) and v < val): return False
                elif op == ">=":
                    if not (isinstance(v, (int,float)) and v >= val): return False
                elif op == "<=":
                    if not (isinstance(v, (int,float)) and v <= val): return False
                else:
                    return False
            return True
        return TableQuery([r for r in self.rows if check(r)])


# ------------------------------
# FolderDB: auto detect tables
# ------------------------------
class FolderDB:
    def __init__(self, base_path="raw_dat",base_metadata="metadata.json"):
        self.base_path = base_path
        self.base_metadata=base_metadata
        self.tables = {}  # name -> list of rows
        self._load()
        self._attach_tables()
        self.sql_engine = SQLParserAdvanced(self)  # attach SQL engine
        

    def _load(self):
        for folder in sorted(os.listdir(self.base_path)):
            folder_path = os.path.join(self.base_path, folder)
            json_file = os.path.join(folder_path, self.base_metadata)
            if not (os.path.isdir(folder_path) and os.path.exists(json_file)):
                continue
            try:
                with open(json_file, "r") as f:
                    data = json.load(f)
            except Exception as e:
                print(f"skip {json_file}: {e}")
                continue

            for table_name, table_data in data.items():
                self.tables.setdefault(table_name, [])
                row = {"iid": folder}
                if isinstance(table_data, dict):
                    row.update(table_data)
                else:
                    # if not dict, store under 'value'
                    row["value"] = table_data
                self.tables[table_name].append(row)

    def _attach_tables(self):
        for name, rows in self.tables.items():
            setattr(self, name, TableQuery(rows))

    # convenience SQL method
    def sql(self, q):
        return TableQuery(self.sql_engine.run(q))


# ------------------------------
# SQL Advanced Parser
# ------------------------------

Token = namedtuple("Token", ["type", "value"])

class SQLParserAdvanced:
    """
    Advanced SQL parser supporting:
      - SELECT with aggregate functions and aliases
      - FROM with table alias
      - JOIN ... USING(key) with optional alias
      - WHERE boolean expressions with parentheses, AND/OR/NOT
      - GROUP BY
      - HAVING with boolean expressions
      - ORDER BY <col> [ASC|DESC]
      - LIMIT n
    """
    def __init__(self, db: FolderDB):
        self.db = db

    # --------------------
    # Public run method
    # --------------------
    def run(self, sql_text):
        self.sql = sql_text.strip()
        # Normalize whitespace but keep case for identifiers; we'll parse case-insensitively.
        s = self.sql
        # Extract clauses using regex patterns tolerant to newlines
        s_clean = " ".join(s.split())
        s_lower = s_clean.lower()

        # SELECT clause
        m = re.search(r"select\s+(.*?)\s+from\s", s_clean, re.I)
        if not m:
            raise ValueError("Invalid SQL: missing SELECT ... FROM")
        select_clause = m.group(1).strip()

        # FROM (with optional alias)
        m_from = re.search(r"from\s+([a-zA-Z0-9_]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_]+))?", s_clean, re.I)
        if not m_from:
            raise ValueError("Invalid SQL: missing FROM table")
        from_table = m_from.group(1)
        from_alias = m_from.group(2) or from_table

        # Start table object from FolderDB
        if not hasattr(self.db, from_table):
            raise ValueError(f"Unknown table '{from_table}'")
        table = getattr(self.db, from_table)
        alias_map = {from_alias: table}

        # JOINS: find all "JOIN <table> [AS alias] USING(col)"
        joins = re.findall(r"join\s+([a-zA-Z0-9_]+)(?:\s+(?:as\s+)?([a-zA-Z0-9_]+))?\s+using\s*\(\s*([a-zA-Z0-9_]+)\s*\)", s_clean, re.I)
        for join_table, join_alias, join_key in joins:
            if not hasattr(self.db, join_table):
                raise ValueError(f"Unknown join table '{join_table}'")
            jt = getattr(self.db, join_table)
            # track alias
            join_alias = join_alias or join_table
            alias_map[join_alias] = jt
            # perform join on key; join table onto current table
            table = table.join(jt, on=join_key)

        # WHERE clause
        where_clause = self._extract_between(s_clean, r"where\s+", r"(group by|having|order by|limit|$)")
        if where_clause:
            cond_expr = self._parse_boolean_expression(where_clause)
            table = self._apply_filter_expr(table, cond_expr)

        # GROUP BY clause
        group_clause = self._extract_between(s_clean, r"group by\s+", r"(having|order by|limit|$)")
        grouped = False
        if group_clause:
            group_cols = [c.strip() for c in group_clause.split(",")]
            table = table.group_by(*group_cols)
            grouped = True

        # HAVING clause
        having_clause = self._extract_between(s_clean, r"having\s+", r"(order by|limit|$)")
        if having_clause:
            having_expr = self._parse_boolean_expression(having_clause)
            # When HAVING needs to run on grouped rows, our TableQuery.having expects aggregated-style rows
            table = self._apply_filter_expr(table, having_expr, is_having=True)

        # ORDER BY
        ob_match = re.search(r"order by\s+([a-zA-Z0-9_]+)(?:\s+(asc|desc))?", s_clean, re.I)
        if ob_match:
            col = ob_match.group(1)
            direction = ob_match.group(2) or "asc"
            table = table.order_by(col, desc=(direction.lower()=="desc"))

        # LIMIT
        lm = re.search(r"limit\s+([0-9]+)", s_clean, re.I)
        if lm:
            n = int(lm.group(1))
            table = table.limit(n)

        # SELECT projection
        result_rows = table
        if select_clause.strip() != "*":
            select_items = self._parse_select_items(select_clause)
            # If grouped results (group_by called), we need to compute aggregates per group
            if grouped:
                projected = []
                for gr in result_rows.all():   # gr is group dict with _rows etc.
                    row_out = {}
                    for item in select_items:
                        if item['type'] == 'agg':
                            func = item['func']
                            col = item['col']
                            alias = item['alias']
                            value = self._compute_group_agg(gr, func, col)
                            row_out[alias] = value
                        elif item['type'] == 'col':
                            name = item['name']
                            alias = item['alias']
                            # if name present in group dict (like COUNT or SUM_col), use it; else try first row's value
                            if name in gr:
                                row_out[alias] = gr[name]
                            else:
                                # take from first row in group
                                rows = gr.get("_rows", [])
                                row_out[alias] = rows[0].get(name) if rows else None
                    projected.append(row_out)
                return projected
            else:
                # not grouped
                # If select contains aggregate functions we should compute them over whole table and return single row
                has_agg = any(item['type']=='agg' for item in select_items)
                if has_agg:
                    single = {}
                    rows = result_rows.all()
                    for item in select_items:
                        if item['type']=='agg':
                            single[item['alias']] = self._compute_aggregate_over_rows(item['func'], item['col'], rows)
                        else:
                            # pick first row's column (or None)
                            single[item['alias']] = rows[0].get(item['name']) if rows else None
                    return [single]
                else:
                    # simple column projection for every row
                    cols = [item['name'] for item in select_items]
                    return [ {item['alias']: r.get(item['name']) for item in select_items} for r in result_rows.all() ]
        else:
            return result_rows.all()

    # --------------------
    # Helpers: extract between regex start and stop token
    # --------------------
    def _extract_between(self, s, start_pat, stop_pat):
        m = re.search(start_pat + r"(.*?)(?=\s*"+stop_pat+")", s, re.I)
        if m:
            return m.group(1).strip()
        return None

    # --------------------
    # SELECT parsing: handle functions and aliases
    # --------------------
    def _parse_select_items(self, clause):
        # split top-level commas (not inside parentheses)
        items = self._split_top_level_commas(clause)
        parsed = []
        for it in items:
            it = it.strip()
            # alias AS or implicit alias
            m_alias = re.match(r"^(.*?)(?:\s+as\s+|\s+)([a-zA-Z0-9_]+)$", it, re.I)
            if m_alias:
                expr, alias = m_alias.group(1).strip(), m_alias.group(2)
            else:
                expr, alias = it, None

            # function?
            m_func = re.match(r"^([A-Za-z0-9_]+)\s*\(\s*([A-Za-z0-9_*]+)?\s*\)$", expr)
            if m_func:
                func = m_func.group(1).upper()
                col = m_func.group(2)  # may be None for COUNT(*)
                alias = alias or (f"{func}_{col or '*'}")
                parsed.append({"type":"agg","func":func,"col":col,"alias":alias})
            else:
                # column name
                name = expr
                alias = alias or name
                parsed.append({"type":"col","name":name,"alias":alias})
        return parsed

    def _split_top_level_commas(self, s):
        parts = []
        cur = []
        depth = 0
        for ch in s:
            if ch == '(':
                depth +=1
            elif ch == ')':
                depth -=1
            if ch == ',' and depth==0:
                parts.append(''.join(cur))
                cur = []
            else:
                cur.append(ch)
        if cur:
            parts.append(''.join(cur))
        return parts

    # --------------------
    # Aggregation helpers
    # --------------------
    def _compute_group_agg(self, group_dict, func, col):
        rows = group_dict.get("_rows", [])
        return self._compute_aggregate_over_rows(func, col, rows)

    def _compute_aggregate_over_rows(self, func, col, rows):
        func = func.upper()
        if func == "COUNT":
            if col is None or col == "*":
                return len(rows)
            else:
                return sum(1 for r in rows if r.get(col) is not None)
        vals = [r.get(col) for r in rows if isinstance(r.get(col), (int,float))]
        if not vals:
            return None
        if func == "SUM":
            return sum(vals)
        if func == "MIN":
            return min(vals)
        if func == "MAX":
            return max(vals)
        if func == "AVG":
            return sum(vals) / len(vals)
        return None

    # --------------------
    # WHERE/HAVING expression parsing (tokenize -> shunting-yard -> RPN -> AST eval)
    # --------------------
    def _parse_boolean_expression(self, text):
        tokens = self._tokenize_expr(text)
        rpn = self._to_rpn(tokens)
        return rpn  # store RPN list to evaluate later

    def _apply_filter_expr(self, table: TableQuery, rpn_expr, is_having=False):
        """
        Evaluate rpn_expr on either normal rows (table.rows)
        or on grouped rows (which are dicts with aggregated keys)
        """
        def eval_row(row):
            stack = []
            for tk in rpn_expr:
                if tk.type == "LITERAL":
                    stack.append(tk.value)
                elif tk.type == "IDENT":
                    stack.append(row.get(tk.value))
                elif tk.type == "OP":
                    if tk.value in ("=", "!=", ">", "<", ">=", "<="):
                        b = stack.pop(); a = stack.pop()
                        res = False
                        try:
                            if tk.value == "=":
                                res = (a == b)
                            elif tk.value == "!=":
                                res = (a != b)
                            elif tk.value == ">":
                                res = (a is not None and b is not None and a > b)
                            elif tk.value == "<":
                                res = (a is not None and b is not None and a < b)
                            elif tk.value == ">=":
                                res = (a is not None and b is not None and a >= b)
                            elif tk.value == "<=":
                                res = (a is not None and b is not None and a <= b)
                        except Exception:
                            res = False
                        stack.append(res)
                    elif tk.value == "CONTAINS":
                        b = stack.pop(); a = stack.pop()
                        stack.append(isinstance(a, str) and isinstance(b, str) and b.lower() in a.lower())
                    elif tk.value == "IN":
                        b = stack.pop(); a = stack.pop()
                        # b should be list
                        stack.append(a in b if b is not None else False)
                    elif tk.value == "NOT IN":
                        b = stack.pop(); a = stack.pop()
                        stack.append(not (a in b))
                    elif tk.value == "AND":
                        b = stack.pop(); a = stack.pop()
                        stack.append(bool(a) and bool(b))
                    elif tk.value == "OR":
                        b = stack.pop(); a = stack.pop()
                        stack.append(bool(a) or bool(b))
                    elif tk.value == "NOT":
                        a = stack.pop()
                        stack.append(not bool(a))
                    else:
                        raise ValueError("Unknown op " + tk.value)
                else:
                    raise ValueError("Unknown token type " + tk.type)
            return bool(stack[-1]) if stack else False

        filtered = [r for r in table.all() if eval_row(r)]
        return TableQuery(filtered)

    # --------------------
    # Expression tokenizer
    # --------------------
    def _tokenize_expr(self, text):
        s = text.strip()
        i = 0
        tokens = []
        while i < len(s):
            ch = s[i]
            if ch.isspace():
                i+=1; continue
            if ch == '(':
                tokens.append(Token("LPAREN","(")); i+=1; continue
            if ch == ')':
                tokens.append(Token("RPAREN",")")); i+=1; continue
            # multi-char operators and keywords
            # try to match >=, <=, !=
            if s[i:i+2] in (">=", "<=", "!="):
                tokens.append(Token("OP", s[i:i+2])); i+=2; continue
            if s[i] in ("=", ">", "<"):
                tokens.append(Token("OP", s[i])); i+=1; continue
            # keywords: AND OR NOT CONTAINS IN
            m = re.match(r"(and|or|not)\b", s[i:], re.I)
            if m:
                kw = m.group(1).upper()
                if kw == "AND":
                    tokens.append(Token("OP","AND"))
                elif kw == "OR":
                    tokens.append(Token("OP","OR"))
                elif kw == "NOT":
                    tokens.append(Token("OP","NOT"))
                i += len(m.group(0)); continue
            m = re.match(r"contains\b", s[i:], re.I)
            if m:
                tokens.append(Token("OP","CONTAINS")); i += len(m.group(0)); continue
            m = re.match(r"in\b", s[i:], re.I)
            if m:
                tokens.append(Token("OP","IN")); i += len(m.group(0)); continue
            # string literal
            if ch in ("'", '"'):
                q = ch
                j = i+1
                buf = []
                while j < len(s):
                    if s[j] == q and s[j-1] != "\\":
                        break
                    buf.append(s[j]); j+=1
                val = "".join(buf)
                tokens.append(Token("LITERAL", val))
                i = j+1
                continue
            # parenthesized list for IN: we treat '(' as LPAREN and parse items as literals/idents later
            # identifier or number or comma
            m = re.match(r"[A-Za-z0-9_.*]+", s[i:])
            if m:
                tok = m.group(0)
                # numeric?
                if re.match(r"^\d+(\.\d+)?$", tok):
                    num = float(tok) if '.' in tok else int(tok)
                    tokens.append(Token("LITERAL", num))
                else:
                    # treat '*' as ident or COUNT(*)
                    tokens.append(Token("IDENT", tok))
                i += len(tok)
                continue
            # comma
            if ch == ',':
                tokens.append(Token("COMMA", ",")); i+=1; continue
            # fallback
            raise ValueError(f"Unexpected char in WHERE: {ch} at pos {i} in '{s}'")
        # Convert sequences like IDENT LPAREN ... RPAREN into function/values? We'll handle IN lists in shunting-yard by reading LPAREN and extracting list tokens.
        # For convenience convert IDENT tokens that are uppercase COUNT/ SUM etc followed by LPAREN to function handling in RPN step
        return tokens

    # --------------------
    # Shunting-yard to RPN
    # --------------------
    def _to_rpn(self, tokens):
        out = []
        opstack = []

        # operator precedence
        prec = {
            "NOT": 5,
            "CONTAINS": 4,
            "IN": 4,
            "NOT IN": 4,
            "=": 4, "!=":4, ">":4, "<":4, ">=":4, "<=":4,
            "AND": 2,
            "OR": 1
        }

        i = 0
        while i < len(tokens):
            tk = tokens[i]
            if tk.type == "LITERAL":
                out.append(tk)
                i+=1; continue
            if tk.type == "IDENT":
                # look ahead: IDENT LPAREN ... RPAREN could be a list (for IN) or function; for WHERE we expect maybe a bare IDENT (column) or COUNT(*)
                out.append(Token("IDENT", tk.value))
                i+=1; continue
            if tk.type == "OP":
                op = tk.value
                # special handling for 'not' preceding 'in' -> 'NOT IN'
                if op == "NOT":
                    # if next token operator IN, we will combine later. For simplicity we'll push NOT operator.
                    while opstack and opstack[-1].type=="OP" and prec.get(opstack[-1].value,0) > prec.get(op,0):
                        out.append(opstack.pop())
                    opstack.append(tk)
                    i+=1; continue
                if op.upper() == "IN":
                    # IN expects next token to be LPAREN then literal(s) then RPAREN
                    # find list between parens
                    if i+1 >= len(tokens) or tokens[i+1].type != "LPAREN":
                        raise ValueError("IN must be followed by (list)")
                    # read until matching RPAREN
                    j = i+2
                    vals = []
                    current = []
                    while j < len(tokens):
                        if tokens[j].type == "RPAREN":
                            if current:
                                # current may contain IDENT or LITERAL
                                # build value
                                if len(current) == 1:
                                    v = current[0].value
                                else:
                                    # join?
                                    v = ''.join([c.value for c in current])
                                vals.append(v)
                                current=[]
                            break
                        if tokens[j].type == "COMMA":
                            if current:
                                v = current[0].value if len(current)==1 else ''.join([c.value for c in current])
                                vals.append(v)
                                current=[]
                            j+=1; continue
                        else:
                            current.append(tokens[j])
                        j+=1
                    # push a LITERAL list onto out
                    # convert IDENT literals to their raw value (strings)
                    parsed_vals = []
                    for it in vals:
                        # if numeric string
                        if isinstance(it, (int,float)):
                            parsed_vals.append(it)
                        else:
                            # strip quotes if present
                            s = str(it)
                            s = s.strip("'\"")
                            if s.isdigit():
                                parsed_vals.append(int(s))
                            else:
                                try:
                                    f = float(s); parsed_vals.append(f)
                                except:
                                    parsed_vals.append(s)
                    out.append(Token("LITERAL", parsed_vals))
                    i = j+1
                    # push IN operator to opstack according to precedence
                    while opstack and opstack[-1].type=="OP" and prec.get(opstack[-1].value,0) >= prec.get("IN",0):
                        out.append(opstack.pop())
                    opstack.append(Token("OP","IN"))
                    continue
                # for normal ops:
                while opstack and opstack[-1].type=="OP" and prec.get(opstack[-1].value,0) >= prec.get(op,0):
                    out.append(opstack.pop())
                opstack.append(tk)
                i+=1; continue
            if tk.type == "LPAREN":
                opstack.append(tk); i+=1; continue
            if tk.type == "RPAREN":
                while opstack and opstack[-1].type != "LPAREN":
                    out.append(opstack.pop())
                if not opstack:
                    raise ValueError("Mismatched parentheses")
                opstack.pop()  # pop LPAREN
                i+=1; continue
            # others
            i+=1

        while opstack:
            t = opstack.pop()
            if t.type in ("LPAREN","RPAREN"):
                raise ValueError("Mismatched parentheses")
            out.append(t)
        # post-process: map OP tokens for words and ensure token types
        rpn = []
        for t in out:
            if t.type == "OP":
                rpn.append(Token("OP", t.value.upper()))
            else:
                rpn.append(t)
        return rpn

# ------------------------------
# Usage example and quick test
# ------------------------------
if __name__ == "__main__":
    # create sample data if raw_dat doesn't exist
    import shutil
    base = "raw_dat"
    if os.path.exists(base):
        shutil.rmtree(base)
    os.makedirs(base, exist_ok=True)
    # create 5 sample folders
    for i in range(1,6):
        fname = f"data_sample_{i}"
        path = os.path.join(base, fname)
        os.makedirs(path, exist_ok=True)
        data = {
            "email_dict": {
                "email_col1": f"hello_{i}",
                "email_col2": f"subject_{i}",
                "score": i * 10
            },
            "attachment": {
                "att_col1": f"file_{i}.pdf",
                "size_kb": i * 100
            },
            "RFP_DETAILS": {
                "amount": i * 1000,
                "client": f"client_{i%3}"
            }
        }
        with open(os.path.join(path, "data.json"), "w") as f:
            json.dump(data, f, indent=2)

    # load db
    db = FolderDB(base)

    print("Tables detected:", list(db.tables.keys()))
    print("\n--- Python API examples ---")
    print("email rows:", db.email_dict.all())
    print("attachments size > 200:", db.attachment.where(("size_kb", ">", 200)).all())

    print("\n--- SQL API examples ---")
    q1 = "SELECT iid, email_col1 FROM email_dict WHERE email_col1 contains 'hello' ORDER BY iid DESC LIMIT 3"
    print("Q1:", db.sql(q1))

    q2 = """
    SELECT iid, COUNT(*) 
    FROM email_dict
    GROUP BY iid
    HAVING COUNT > 0
    ORDER BY iid
    LIMIT 5
    """
    print("Q2:", db.sql(q2))

    q3 = """
    SELECT iid, SUM_amount
    FROM email_dict
    JOIN RFP_DETAILS USING(iid)
    GROUP BY iid
    HAVING SUM_amount > 1000
    ORDER BY SUM_amount DESC
    LIMIT 10
    """
    print("Q3:", db.sql(q3))

    q4 = "SELECT SUM(amount) AS total_amount FROM RFP_DETAILS"
    print("Q4:", db.sql(q4))

    q5 = "SELECT iid, amount FROM RFP_DETAILS WHERE amount >= 3000"
    print("Q5:", db.sql(q5))

    # complex where with parentheses and AND/OR/NOT
    q6 = "SELECT iid, email_col1 FROM email_dict WHERE (email_col1 contains 'hello' AND score >= 20) OR (email_col1 contains 'hello_1')"
    print("Q6:", db.sql(q6))

    print("\nDone.")
