import re
import os
import sys
import csv
import sqlparse
import tempfile
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from contextlib import contextmanager

from pygments.lexers.sql import SqlLexer
from prompt_toolkit import PromptSession
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit import print_formatted_text, HTML


class CharField(str):
    def __new__(cls, name, unique, length):
        class Char(str):
            __qualname__ = f'CHAR({length})'

            def __new__(cls, text=''):
                if not isinstance(text, str):
                    raise ValueError('Invalid value for CHAR field')

                cls.name = name
                cls.unique = unique
                cls.length = int(length)

                if len(text) > cls.length:
                    raise ValueError(f'String is longer than {cls.length}')

                return super().__new__(cls, text
                                       .replace('\r\n', '\\n')
                                       .replace('\n', '\\n'))

        return Char


class IntegerField(int):
    def __new__(cls, name, unique):
        class Integer(int):
            __qualname__ = 'INTEGER'

            def __new__(cls, number=0):
                cls.name = name
                cls.unique = unique

                try:
                    return super().__new__(cls, number)
                except ValueError:
                    raise ValueError(f'Invalid data for {cls.name} field ({number})')

        return Integer


class BooleanField(int):
    def __new__(cls, name, unique):
        class Boolean(int):
            __qualname__ = 'BOOLEAN'

            def __new__(cls, boolean=False):
                cls.name = name
                cls.unique = unique

                if isinstance(boolean, str) and not boolean.isnumeric():
                    boolean = (boolean in ('true', 'True'))

                try:
                    _ = super().__new__(cls, boolean)
                except ValueError:
                    raise ValueError('Inavlid data for boolean field')

                if not (_ == 0 or _ == 1):
                    raise ValueError('Booleans can be either 0 or 1')

                return _

        return Boolean


class TimestampField(datetime):
    def __new__(cls, name, unique):
        class Timestamp(datetime):
            __qualname__ = 'TIMESTAMP'

            def __new__(cls, datetime_str=''):
                cls.name = name
                cls.unique = unique

                if not datetime_str:
                    d = datetime.utcnow()
                else:
                    d = datetime.fromisoformat(datetime_str)

                return super().__new__(cls,
                                       year=d.year,
                                       month=d.month,
                                       day=d.day,
                                       hour=d.hour,
                                       minute=d.minute,
                                       second=d.second,
                                       tzinfo=d.tzinfo)

        return Timestamp


class Table(OrderedDict):
    def __init__(self, table_name: str, fields: dict, data_dir: Path):
        self.table_name = table_name
        self._file = data_dir / f'{table_name}.txt'
        self._file.touch(exist_ok=True)
        self['id'] = IntegerField('id', unique=True)
        self.update(fields)
        self._check_fields()

    def __repr__(self):
        return f'<Table {self.table_name} ({super().__repr__()})>'

    def _check_fields(self):
        with self.get_reader(no_header=False) as reader:
            try:
                header = next(reader)
            except StopIteration:
                header = []

            for col_name in header:
                if not self.get(col_name):
                    self._rem_field(col_name)

            field_idx = 0
            for field_name in self:
                if field_name not in header:
                    self._add_field(field_name, field_idx, self[field_name])

                elif field_name != header[field_idx]:
                    self._shift_field(field_name, field_idx)

                field_idx += 1

    def _add_field(self, field_name, field_idx, field):
        with self.get_reader(no_header=False) as reader:
            with tempfile.NamedTemporaryFile('w+') as tmp_file:
                tmp_writer = csv.writer(tmp_file, delimiter=' ', quotechar='"',
                                        quoting=csv.QUOTE_ALL, lineterminator='\n')

                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                header.insert(field_idx, field_name)
                tmp_writer.writerow(header)

                for row in reader:
                    row.insert(field_idx, field())
                    tmp_writer.writerow(row)

                tmp_file.seek(0)
                with open(self._file, 'w') as f:
                    f.write(tmp_file.read())

    def _rem_field(self, field_name):
        with self.get_reader(no_header=False) as reader:
            with tempfile.NamedTemporaryFile('w+') as tmp_file:
                tmp_writer = csv.writer(tmp_file, delimiter=' ', quotechar='"',
                                        quoting=csv.QUOTE_ALL, lineterminator='\n')

                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                field_idx = header.index(field_name)
                header.remove(field_name)
                tmp_writer.writerow(header)

                for row in reader:
                    del row[field_idx]
                    tmp_writer.writerow(row)

                tmp_file.flush()
                tmp_file.seek(0)
                with open(self._file, 'w') as f:
                    f.write(tmp_file.read())

    def _shift_field(self, field_name, to_idx):
        with self.get_reader(no_header=False) as reader:
            with tempfile.NamedTemporaryFile('w+') as tmp_file:
                tmp_writer = csv.writer(tmp_file, delimiter=' ', quotechar='"',
                                        quoting=csv.QUOTE_ALL, lineterminator='\n')

                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                field_idx = header.index(field_name)
                header.insert(to_idx, header.pop(field_idx))
                tmp_writer.writerow(header)

                for row in reader:
                    row.insert(to_idx, row.pop(field_idx))
                    tmp_writer.writerow(row)

                tmp_file.flush()
                tmp_file.seek(0)
                with open(self._file, 'w') as f:
                    f.write(tmp_file.read())

    def _delete_lines(self, lines: list):
        with self.get_reader(no_header=False) as reader:
            with tempfile.NamedTemporaryFile('w+') as tmp_file:
                tmp_writer = csv.writer(tmp_file, delimiter=' ', quotechar='"',
                                        quoting=csv.QUOTE_ALL, lineterminator='\n')

                header = next(reader)
                tmp_writer.writerow(header)
                line_c = 1  # start from header
                for row in reader:
                    line_c += 1
                    if line_c in lines:
                        continue
                    tmp_writer.writerow(row)

                tmp_file.flush()
                tmp_file.seek(0)
                with open(self._file, 'w') as f:
                    f.write(tmp_file.read())

    def _update_line(self, line: int, values: list):
        with self.get_reader(no_header=False) as reader:
            with tempfile.NamedTemporaryFile('w+') as tmp_file:
                tmp_writer = csv.writer(tmp_file, delimiter=' ', quotechar='"',
                                        quoting=csv.QUOTE_ALL, lineterminator='\n')

                header = next(reader)
                tmp_writer.writerow(header)
                line_c = 1  # start from header
                for row in reader:
                    line_c += 1
                    if line_c == line:
                        tmp_writer.writerow(values)
                        continue
                    tmp_writer.writerow(row)

                tmp_file.flush()
                tmp_file.seek(0)
                with open(self._file, 'w') as f:
                    f.write(tmp_file.read())

    def _compile_condition(self, condition: list):
        new_condition = []
        for i, p in enumerate(condition):
            if p in ('==', '!='):
                left = condition[i-1]
                right = condition[i+1]
                try:
                    new_condition.insert(i-1, f'row[{list(self).index(left)}]')
                except ValueError:
                    raise ValueError(f'Column {left} doesn\'t exist')
                new_condition.insert(i, p)
                if right.startswith("'") and right.endswith("'"):
                    right = right[1:-1]
                right = right.replace("'", "\\'")
                new_condition.insert(i+1, f'\'{self[left](right)}\'')
            elif p.lower() in ('or', 'and', '(', ')'):
                new_condition.insert(i, p.lower())

        return ' '.join(new_condition)

    def _search(self, condition, reverse=False):
        condition = self._compile_condition(condition)
        with self.get_reader(no_header=not reverse, reverse=reverse) as reader:
            line_c = 1
            for row in reader:
                line_c += 1
                try:
                    if eval(condition, {"row": row}):
                        yield line_c, self._parse_values(row)
                except SyntaxError:
                    raise ValueError('Error in where clause syntax')

    def _parse_values(self, row):
        idx = 0
        parsed = OrderedDict()
        for field_name, field in self.items():
            try:
                parsed[field_name] = field(row[idx])
            except IndexError:
                raise ValueError(f'field {field_name} not determined')
            idx += 1
        return parsed

    def _check_for_uniqueness(self, fields: OrderedDict, to_update=False):
        cond = []
        fake_fields = fields.copy()
        check = False

        if to_update is True:
            cond.extend(['id', '!=', f'{fake_fields["id"]}', 'AND', '('])
            del fake_fields['id']

        for field_name, field in fake_fields.items():
            if field.unique is True:
                check = True
                cond.extend([field_name, '==', f'{field}', 'OR'])
        cond = cond[:-1]  # Remove additional or at the end

        if to_update is True:
            cond.append(')')

        if check:
            search = self._search(cond)
            for line, row in search:
                for field_name in row:
                    if row[field_name] == fields[field_name]:
                        raise ValueError(f'duplicate data for {field_name} field')

    def _reverse_db_csv(self, file):
        part = ''
        for block in self._reverse_file_blocks(file):
            for c in reversed(block):
                if c == '\n' and part:
                    yield part[::-1]
                    part = ''
                part += c

        # it won't yield last part to pass header
        # if part:
        #    yield part[::-1]

    def _reverse_file_blocks(self, file, blocksize=4096):
        "Generate blocks of file's contents in reverse order."
        file.seek(0, os.SEEK_END)
        here = file.tell()
        while 0 < here:
            delta = min(blocksize, here)
            here -= delta
            file.seek(here, os.SEEK_SET)
            yield file.read(delta)

    @contextmanager
    def get_writer(self, reset=False):
        f = open(self._file, 'a' if reset is False else 'w')
        try:
            yield csv.writer(f, delimiter=' ', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
        finally:
            f.close()

    @contextmanager
    def get_reader(self, no_header=True, reverse=False):
        f = open(self._file, 'r')

        if not no_header and reverse:
            f = self._reverse_db_csv(f)

        elif no_header and reverse:
            raise EnvironmentError(
                'You cannot set both no_header and reverse True'
                '\nin general, header won\'t be read in reverse mode'
            )

        if no_header:
            next(f)  # pass header

        try:
            reader = csv.reader(f, delimiter=' ')
            yield reader
        finally:
            f.close()

    @property
    def last_id(self):
        if not getattr(self, '_last_id', None):
            with self.get_reader(no_header=True) as reader:
                row = None
                for row in reader:
                    pass
                if row:
                    self._last_id = self._parse_values(row).get('id')
                else:
                    self._last_id = 0
        return self._last_id

    @last_id.setter
    def last_id(self, value):
        self._last_id = value

    def db_insert(self, values: list):
        next_id = self.last_id + 1
        values.insert(0, next_id)  # auto increament
        parsed = self._parse_values(values)
        self._check_for_uniqueness(parsed)

        with self.get_writer() as writer:
            writer.writerow(parsed.values())
            self.last_id += 1

        return parsed['id']

    def db_delete(self, where: list):
        search = self._search(where)
        lines = [r[0] for r in search]
        self._delete_lines(lines)

    def db_select(self, where: list = None, limit: int = None, reverse: bool = False):
        if where is None:
            with self.get_reader(no_header=not reverse, reverse=reverse) as reader:
                if limit is not None:
                    results = []
                    i = 0
                    for row in reader:
                        if i == limit:
                            break
                        i += 1
                        results.append(self._parse_values(row))
                    return results

                return [self._parse_values(row) for row in reader]

        search = self._search(where, reverse=reverse)
        if limit is not None:
            results = []
            i = 0
            for line, row in search:
                if i == limit:
                    break
                i += 1
                results.append(row)
            return results

        return [r[1] for r in search]  # values

    def db_update(self, where: list, values: list):
        search = self._search(where)
        results = []
        for line, vals in search:
            results.append(vals['id'])
            parsed = self._parse_values([vals['id']] + values)
            self._check_for_uniqueness(parsed, to_update=True)
            self._update_line(line, parsed.values())
        return results


class Database(OrderedDict):
    def __init__(self, db_name, schema_file):
        normalized_name = re.sub(r'\s+', "_", db_name)
        self.db_name = db_name
        self._data_dir = Path(f'{normalized_name}_data').absolute()
        self._data_dir.mkdir(exist_ok=True)
        self._initialize_schema(schema_file)

    def __repr__(self):
        return f'<Database {self.db_name} ({super().__repr__()})>'

    def _initialize_schema(self, schema_file):
        with open(schema_file, 'r') as f:
            emp_line_btw = 0
            current_table = ''
            current_fields = OrderedDict()
            line_c = 0
            while True:
                raw_line = f.readline()
                line_c += 1
                line = re.sub(r'\s+', ' ', raw_line.strip())

                if not line:
                    if emp_line_btw > 5:
                        if current_table:  # initialize latest table
                            self._initialize_table(current_table, current_fields)
                        break

                    emp_line_btw += 1
                    continue

                words = line.split()
                words_len = len(words)
                if words_len == 1:  # table
                    if current_table:  # initialize previous table
                        self._initialize_table(current_table, current_fields)
                        current_table = ''
                        current_fields = OrderedDict()

                    current_table = words[0]
                    if re.search(r'\s+', current_table):
                        raise ValueError(
                            f'table name cannot contain spaces, line {line_c}')

                elif words_len == 3 and current_table:  # fields
                    field_name, unique, field_type = words
                    try:
                        field = self._initialize_field(
                            current_table, field_name, unique, field_type)
                        current_fields[field_name] = field
                    except ValueError as e:
                        raise ValueError(f'schema error in line {line_c}: {e}')

                else:
                    raise ValueError(f'bad schema in line {line_c}')

    def _initialize_table(self, table_name, fields):
        self[table_name] = Table(table_name, fields, self._data_dir)
        return self[table_name]

    def _initialize_field(self, table_name, field_name, unique, field_type):
        if re.search(r'\s+', field_name):
            raise ValueError('field\'s name cannot contain spaces')

        if unique == 'false':
            unique = False
        elif unique == 'true':
            unique = True
        else:
            raise ValueError('invalid value to determine uniqueness')

        field_type = field_type.lower()
        if field_type.startswith('char'):
            if not (m := re.findall(r'char\((\d+)\)', field_type)):
                raise ValueError('length of char must be specified, like CHAR(16)')
            length = m[0]
            field = CharField(field_name, unique, length)

        elif field_type == 'integer':
            field = IntegerField(field_name, unique)

        elif field_type == 'boolean':
            field = BooleanField(field_name, unique)

        elif field_type == 'timestamp':
            field = TimestampField(field_name, unique)

        else:
            raise ValueError('unknown type')

        return field

    def _parse_where(self, where):
        cond = []
        for token in where[1:]:  # start after where keyword
            if token.ttype == sqlparse.tokens.Whitespace:
                continue

            elif isinstance(token, sqlparse.sql.Comparison):
                op = token.value \
                    .replace(token.left.value, '') \
                    .replace(token.right.value, '') \
                    .strip()
                right = token.left.value
                left = token.right.value
                cond.extend([right, op, left])
            elif token.match(sqlparse.tokens.Keyword, ['AND', 'OR']):
                cond.append(token.value)
            else:
                raise ValueError('Error in where clause syntax')
        return cond

    def _parse_values(self, values):
        fields = []
        for token in values[1:]:  # start after value keyword
            if isinstance(token, sqlparse.sql.Parenthesis):
                for vals in token:
                    if vals.ttype == sqlparse.tokens.Punctuation:
                        continue
                    try:
                        for val in vals:
                            if val.ttype in (sqlparse.tokens.Punctuation,
                                             sqlparse.tokens.Whitespace):
                                continue
                            v = val.value
                            if v.startswith("'") and v.endswith("'"):
                                v = v[1:-1]
                            fields.append(v)
                    except TypeError:
                        v = vals.value
                        if v.startswith("'") and v.endswith("'"):
                            v = v[1:-1]
                        fields.append(v)

        return fields

    def _parse_select(self, statement, limit=None, reverse=False):
        st = filter(lambda t: t.ttype != sqlparse.tokens.Whitespace, statement)
        try:
            assert next(st).match(sqlparse.tokens.Keyword.DML, ['SELECT'])
            assert next(st).match(sqlparse.tokens.Keyword, ['FROM'])

            table = next(st)
            assert type(table) == sqlparse.sql.Identifier
            try:
                table = self[table.value]
            except KeyError:
                raise ValueError(f'table {table.value} doesn\'t exist')
        except (StopIteration, AssertionError):
            raise ValueError("Error in query syntax")

        try:
            where = self._parse_where(next(st))
        except StopIteration:
            where = None

        return table.db_select(where, limit=limit, reverse=reverse)

    def _parse_delete(self, statement):
        st = filter(lambda t: t.ttype != sqlparse.tokens.Whitespace, statement)
        try:
            assert next(st).match(sqlparse.tokens.Keyword.DML, ['DELETE'])
            assert next(st).match(sqlparse.tokens.Keyword, ['FROM'])

            table = next(st)
            assert type(table) == sqlparse.sql.Identifier
            try:
                table = self[table.value]
            except KeyError:
                raise ValueError(f'table {table.value} doesn\'t exist')
        except (StopIteration, AssertionError):
            raise ValueError("Error in query syntax")

        try:
            where = self._parse_where(next(st))
        except StopIteration:
            where = None

        return table.db_delete(where)

    def _parse_insert(self, statement):
        st = filter(lambda t: t.ttype != sqlparse.tokens.Whitespace, statement)
        try:
            assert next(st).match(sqlparse.tokens.Keyword.DML, ['INSERT'])
            assert next(st).match(sqlparse.tokens.Keyword, ['INTO'])

            table = next(st)
            assert type(table) == sqlparse.sql.Identifier
            try:
                table = self[table.value]
            except KeyError:
                raise ValueError(f'table {table.value} doesn\'t exist')

            values = next(st)
            assert type(values) == sqlparse.sql.Values
            values = self._parse_values(values)

        except (StopIteration, AssertionError):
            raise ValueError("Error in query syntax")

        return table.db_insert(values)

    def _parse_update(self, statement):
        st = filter(lambda t: t.ttype != sqlparse.tokens.Whitespace, statement)
        try:
            assert next(st).match(sqlparse.tokens.Keyword.DML, ['UPDATE'])

            table = next(st)
            assert type(table) == sqlparse.sql.Identifier
            try:
                table = self[table.value]
            except KeyError:
                raise ValueError(f'table {table.value} doesn\'t exist')

            where_n_values = next(st)
            assert type(where_n_values) == sqlparse.sql.Where
            values_idx = None
            for n, i in enumerate(where_n_values):
                if i.match(sqlparse.tokens.Keyword, "VALUES"):
                    values_idx = n
            assert values_idx
            where = self._parse_where(where_n_values[:values_idx])
            values = self._parse_values(where_n_values[values_idx:])

        except (StopIteration, AssertionError):
            raise ValueError("Error in query syntax")

        return table.db_update(where, values)

    def run_query(self, query, select_limit=None, select_reverse=False):
        splited = sqlparse.split(query)
        if not all(p.endswith(';') for p in splited):
            raise ValueError('Query should be ended with ;')

        results = []
        for part in splited:
            part = part.strip(';')
            for statement in sqlparse.parse(part):
                _type = statement.get_type()

                if _type == 'SELECT':
                    results.extend(self._parse_select(statement,
                                                      limit=select_limit,
                                                      reverse=select_reverse))

                elif _type == 'INSERT':
                    results.append(self._parse_insert(statement))

                elif _type == 'UPDATE':
                    results.extend(self._parse_update(statement))

                elif _type == 'DELETE':
                    self._parse_delete(statement)

        return results


class Shell(object):
    def __init__(self, db_name, schema_file):
        self.db_name = db_name
        self.db = Database(db_name, schema_file)
        self.table_names = set(self.db.keys())
        self.table_schemas = {}
        self.column_names = set()
        for table in self.table_names:
            cols = self.db[table].keys()
            self.table_schemas[table] = {
                k: v.__qualname__ for k, v in self.db[table].items()}
            self.column_names.update(cols)

        self.completer = WordCompleter([
            'help',
            'tables',
            'schema',
            'exit',
            'SELECT',
            'FROM',
            'INSERT',
            'INTO',
            'UPDATE',
            'DELETE',
            'WHERE',
            'VALUES',
            *self.table_names,
            *self.column_names,
            '==',
            '!=',
        ], ignore_case=True)

    def run_query(self, query):
        results = self.db.run_query(query)
        c = 1
        for r in results:
            if isinstance(r, dict):
                _ = f'{c}) '
                for k, v in r.items():
                    _ += f'{k}: {v}\t'
                print(_)
                c += 1
            else:
                print(f'{c}) {r}')
                c += 1

    def show_help(self):
        print_formatted_text(
            HTML(
                "<b>help</b>\tShow this message\n"
                "<b>tables</b>\tShow table names\n"
                "<b>schema [table_name]</b>\tShow table's schema\n"
                "<b>exit</b>\tExit the shell\n"
                "\n"
                "<b>Also you can run database queries</b>\n"
                "<i>Example:</i>\n"
                "<i>SELECT FROM persons WHERE id == 1;</i>"
            )
        )

    def show_tables(self):
        c = 1
        _ = ''
        for table in self.table_names:
            _ += f'{c}) {table}\n'
            c += 1
        print(_, end='')

    def show_schema(self, table):
        if not self.table_schemas.get(table):
            raise ValueError(f'table {table} doesn\'t exist')

        c = 1
        _ = ''
        for name, _type in self.table_schemas[table].items():
            _ += f'{c}) {name.ljust(20)}\t\t{_type}\n'
            c += 1
        print(_, end='')

    def run(self):
        session = PromptSession(
            lexer=PygmentsLexer(SqlLexer),
            completer=self.completer,
            complete_while_typing=True,
            bottom_toolbar=HTML('Type <b>exit</b> or press <b>CTRL+D</b> to exit!'),
            vi_mode=True,
        )

        print_formatted_text(
            HTML(
                "<b>Welcome to database shell</b>\n"
                "Type <b>help</b> to get help message."
            )
        )
        while True:
            try:
                cmd = session.prompt(f'[{self.db_name}] > ')
                cmd_lower = cmd.lower()

                if cmd == 'exit':
                    break

                elif cmd == 'help':
                    self.show_help()

                elif cmd == 'tables':
                    self.show_tables()

                elif matches := re.findall(r'^schema (\S+)$', cmd):
                    self.show_schema(matches[0])

                elif cmd_lower.startswith('select') \
                        or cmd_lower.startswith('insert') \
                        or cmd_lower.startswith('delete') \
                        or cmd_lower.startswith('update'):
                    self.run_query(cmd)

                else:
                    raise ValueError('Unknown input')

            except ValueError as err:
                print_formatted_text(HTML(f'<ansired>{err}</ansired>'))
            except KeyboardInterrupt:
                continue  # Control-C pressed. Try again.
            except EOFError:
                break  # Control-D pressed.

        print('GoodBye!')


if __name__ == '__main__':
    if not len(sys.argv) == 3:
        print_formatted_text(HTML(
            '<ansired>db_name and schema_file must be defined</ansired>\n'
            '<i>try to run database file this way:</i>\n'
            '<b>python database.py [db_name] [schema_file]</b>'
        ))
        exit(0)

    _, db_name, schema_file = sys.argv
    shell = Shell(db_name, schema_file)
    shell.run()
