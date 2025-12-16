import pandas as pd
import math
from functools import reduce
from contextlib import contextmanager

# Декоратор для вывода начала и конца выполнения функций
def show_stage(func):
    def wrapper(*args, **kwargs):
        print(f"--- Начало функции: {func.__name__} ---")
        result = func(*args, **kwargs)
        print(f"--- Конец функции: {func.__name__} ---")
        return result
    return wrapper

# Контекстный менеджер для временного файла
@contextmanager
def temp_file(path, text):
    f = open(path, "w", encoding="utf-8")
    try:
        f.write(text)
        yield f
    finally:
        f.close()

# Класс Транслятора
class SlangTranslator:
    def __init__(self):
        self.source_code = []
        self.python_code = []
        self.variables = {}

    @show_stage
    def load_source(self, code_lines):
        self.source_code = code_lines

    def translate_line(self, line):
        line = line.strip()
        if line.startswith("VAR "):
            parts = line.split("=")
            left = parts[0].strip()
            right = parts[1].strip()
            name = left.split()[1]
            self.variables[name] = right
            self.python_code.append(f"{name} = {right}")
        elif line.startswith("PRINT "):
            expr = line[6:].strip()
            self.python_code.append(f"print({expr})")
        elif line.startswith("LOOP "):
            n = line.split()[1].strip(":")
            self.python_code.append(f"for i in range({n}):")
        elif line.startswith("IF "):
            condition = line[3:].strip(":")
            self.python_code.append(f"if {condition}:")
        elif line.startswith("ELSE"):
            self.python_code.append("else:")
        elif line == "ENDLOOP" or line == "ENDIF":
            pass
        else:
            self.python_code.append(f"# неизвестная конструкция: {line}")

    @show_stage
    def translate(self):
        loop_mode = False
        loop_body = []
        if_mode = False
        if_body = []
        for line in self.source_code:
            if line.startswith("LOOP "):
                loop_mode = True
                self.translate_line(line)
            elif line == "ENDLOOP":
                for l in loop_body:
                    self.python_code.append("    " + self.translate_inner_line(l))
                loop_body = []
                loop_mode = False
            elif line.startswith("IF "):
                if_mode = True
                self.translate_line(line)
            elif line == "ENDIF":
                for l in if_body:
                    self.python_code.append("    " + self.translate_inner_line(l))
                if_body = []
                if_mode = False
            else:
                if loop_mode:
                    loop_body.append(line)
                elif if_mode:
                    if_body.append(line)
                else:
                    self.translate_line(line)

    def translate_inner_line(self, line):
        line = line.strip()
        if line.startswith("VAR "):
            parts = line.split("=")
            left = parts[0].strip()
            right = parts[1].strip()
            name = left.split()[1]
            return f"{name} = {right}"
        elif line.startswith("PRINT "):
            expr = line[6:].strip()
            return f"print({expr})"
        else:
            return f"# неизвестная конструкция: {line}"

    @show_stage
    def save_python_code(self, filename="output.py"):
        with open(filename, "w", encoding="utf-8") as f:
            for line in self.python_code:
                f.write(line + "\n")

    @show_stage
    def run_python_code(self):
        code_str = "\n".join(self.python_code)
        exec(code_str, globals(), locals())

# Генератор чисел Фибоначчи
def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b

# Создание тестового исходника
def generate_demo_source():
    code_lines = []
    code_lines.append("VAR x = 5")
    code_lines.append("VAR y = 10")
    code_lines.append("PRINT x + y")
    code_lines.append("LOOP 3:")
    code_lines.append("PRINT x")
    code_lines.append("VAR z = y * 2")
    code_lines.append("PRINT z")
    code_lines.append("ENDLOOP")
    code_lines.append("IF x < y:")
    code_lines.append("PRINT 'x меньше y'")
    code_lines.append("ENDIF")
    return code_lines

# Работа с pandas
@show_stage
def create_dataframe():
    data = {
        "A": [i for i in range(1, 11)],
        "B": [i ** 2 for i in range(1, 11)],
        "C": [str(i) for i in range(1, 11)]
    }
    df = pd.DataFrame(data)
    return df

@show_stage
def python_features_demo():
    nums = list(range(1, 21))
    squares = list(map(lambda x: x * x, nums))
    even = list(filter(lambda x: x % 2 == 0, nums))
    summation = reduce(lambda a, b: a + b, nums)

    print("Числа:", nums)
    print("Квадраты:", squares)
    print("Четные:", even)
    print("Сумма всех чисел:", summation)

    fib_seq = list(fibonacci(15))
    print("Первые 15 чисел Фибоначчи:", fib_seq)

    reversed_words = list(map(lambda w: w[::-1], ["python", "translator", "mini"]))
    print("Перевернутые слова:", reversed_words)

    matrix = [[i * j for j in range(10)] for i in range(10)]
    print("Матрица 10x10 умножений:")
    for row in matrix:
        print(row)

    factorials = {i: math.factorial(i) for i in range(1, 8)}
    print("Факториалы чисел 1..7:", factorials)

    list_of_dicts = [{"id": i, "val": i * 3} for i in range(5)]
    print("Список словарей:", list_of_dicts)

    text = "abcdefghijklmnopqrstuvwxyz"
    letters = [c for c in text if c in "aeiou"]
    print("Гласные буквы:", letters)

    for c in range(5):
        print("Повтор цикла:", c)

    nested = [[x + y for x in range(3)] for y in range(3)]
    print("Вложенный список:", nested)

    result = all(n > 0 for n in nums)
    print("Все числа положительные:", result)

# Основная функция
@show_stage
def main():
    translator = SlangTranslator()
    source = generate_demo_source()
    translator.load_source(source)
    translator.translate()
    translator.save_python_code("translated_program.py")
    translator.run_python_code()

    df = create_dataframe()
    print("Вывод таблицы pandas:")
    print(df.head())

    python_features_demo()

    with temp_file("temp.txt", "Временный текст внутри контекстного менеджера") as f:
        print("Создан временный файл:", f.name)

    big_list = [i * 2 for i in range(100)]
    print("Длина big_list:", len(big_list))
    print("Сумма big_list:", sum(big_list))

    cube_map = {i: i ** 3 for i in range(1, 21)}
    print("Кубы чисел до 20:", cube_map)

    for k, v in cube_map.items():
        if k <= 5:
            print(f"Ключ {k} -> {v}")

    tup_list = [(i, i * i) for i in range(1, 11)]
    print("Список кортежей:", tup_list)

    try:
        risky = 10 / 2
        print("Результат деления:", risky)
    except ZeroDivisionError:
        print("Деление на ноль!")

    lst = [1, 2, 3, 4, 5]
    doubled = [n * 2 for n in lst]
    print("Удвоенный список:", doubled)

    for i in range(3):
        for j in range(3):
            print(f"i={i}, j={j}, сумма={i + j}")

    s = 0
    for i in range(200):
        s += i
    print("Сумма от 0 до 199:", s)

    words = ["apple", "banana", "cherry"]
    word_lengths = {w: len(w) for w in words}
    print("Длины слов:", word_lengths)

    gen_exp = (x ** 2 for x in range(10))
    print("Квадраты через генератор:", list(gen_exp))

    for c in "PYTHON":
        print("Буква:", c)

    print("Работа программы завершена")

if __name__ == "__main__":
    main()
