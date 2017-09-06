

def get_file_contents(file_path):
    try:
        with open(file_path) as file:
            file_contents = file.read()
    except Exception as e:
        print(str(e))
        file_contents = ''
    return file_contents


def read_csv(file_name, headers=True):
    contents = get_file_contents(file_name)
    raw_data = [i.split(',') for i in contents.split('\n') if len(i.split(',')) > 1]
    df = DataBox(raw_data[1:], raw_data[0]) if headers else DataBox(
        raw_data,
        [str(i) for i in range(len(raw_data[0]))])
    return df


class LambdaDict(dict):

    def __init__(self, factory):
        self.factory = factory

    def __missing__(self, key):
        self[key] = self.factory(key)
        return self[key]


class DataBox:

    def __init__(self, data_list, columns=None):
        self.raw = data_list
        self.core = []
        self.base_cols = range(len(self.raw[0]))
        cols = {}
        self.columns = columns if columns is not None else self.base_cols
        for ref, col in zip(self.base_cols, self.columns):
            cols[col] = ref
        self.ref_cols = cols
        self._to_data_structure(data_list)

    def __str__(self):
        string_data = '\t'.join(self.columns) + '\n'
        for row in self.core:
            string_row = ''
            for column in self.base_cols:
                string_row += str(row[column]) + '\t'
            string_data += '\n' + string_row
        return string_data

    def __repr__(self):
        return str(self)

    def to_list(self, core, base_cols=None):
        base_cols = base_cols if base_cols is not None else self.base_cols
        as_list = []
        for row in core:
            row_list = []
            for k in base_cols:
                row_list.append(row[k])
            as_list.append(row_list)
        return as_list

    def loc(self, start, end=None):
        end = end if not None else len(self.core)
        new_core = self.core[start:end]
        return DataBox(self.to_list(new_core), self.columns)

    def set_cols(self, columns):
        cols = {}
        for ref, col in zip(self.base_cols, columns):
            cols[col] = ref
        self.columns = columns
        self.ref_cols = cols

    def head(self, n=10):
        return self.loc(0, n)

    def _to_data_structure(self, data_list):
        for row in data_list:
            self.core.append(self._row_to_dict(row))

    def _row_to_dict(self, row):
        data_line = {}
        for element, column in zip(row, self.base_cols):
            data_line[column] = element
        return data_line

    def show(self):
        print(self)

    def index(self):
        return range(len(self.core))

    def get(self, col):
        result = []
        for row in self.core:
            result.append(row.get(self.ref_cols.get(col)))
        return result

    def apply(self, some_function):
        return [some_function(LambdaDict((lambda l: row[self.ref_cols[l]]))) for row in self.core]

    def lambda_add(self, col, some_function):
        self.add_col(col, self.apply(some_function))

    def filter(self, some_function):
        new_core = [row for row in self.core if some_function(LambdaDict((lambda l: row[self.ref_cols[l]])))]
        return DataBox(self.to_list(new_core), self.columns)

    def diff_col(self, col, some_function=None, first_value=float('nan')):
        some_function = some_function if not None else (lambda i, j: i - j)
        return [first_value] + [some_function(i, j) for i, j in zip(self.get(col)[1:], self.get(col)[:-1])]

    def add_col(self, col, values):
        new_core = []
        new_base = max(self.base_cols) + 1
        for value, row in zip(values, self.core):
            row[new_base] = value
            new_core.append(row)
        self.core = new_core
        self.columns = self.columns + [col]
        self.base_cols = range(new_base + 1)
        self.ref_cols[col] = new_base
