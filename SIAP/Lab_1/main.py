import pandas as pd
import numpy as np
import random
import concurrent.futures

letters = ['A', 'B', 'C', 'D']

for i in range(1, 6):
    data = {
        'Категория': [random.choice(letters) for _ in range(20)],
        'Значение': [round(random.uniform(1, 100), 2) for _ in range(20)]
    }
    pd.DataFrame(data).to_csv(f'file_{i}.csv', index=False, encoding='utf-8-sig')

def process_file(filename):
    df = pd.read_csv(filename)
    res = {}
    for l in letters:
        v = df[df['Категория'] == l]['Значение']
        if len(v) > 0:
            res[l] = (float(np.median(v)), float(np.std(v)))
        else:
            res[l] = (None, None)
    return res

files = [f'file_{i}.csv' for i in range(1, 6)]
all_res = []

with concurrent.futures.ThreadPoolExecutor() as ex:
    fut = [ex.submit(process_file, f) for f in files]
    for f in concurrent.futures.as_completed(fut):
        all_res.append(f.result())

print("Результаты по файлам:")
for i, r in enumerate(all_res, 1):
    print(f"\nФайл {i}:")
    for l in letters:
        print(f"{l}: медиана = {r[l][0]}, отклонение = {r[l][1]}")

median_by_letter = {l: [] for l in letters}
for r in all_res:
    for l in letters:
        if r[l][0] is not None:
            median_by_letter[l].append(r[l][0])

print("\nМедиана из медиан и отклонение медиан:")
for l in letters:
    m = median_by_letter[l]
    if len(m) > 0:
        print(f"{l}: медиана медиан = {float(np.median(m))}, отклонение медиан = {float(np.std(m))}")
    else:
        print(f"{l}: данных нет")
