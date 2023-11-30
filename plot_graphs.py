import matplotlib.pyplot as plt
import numpy as np

def plot_barplots(median_times, queries):
    query_labels = ['Q1', 'Q2', 'Q3', 'Q4']
    libraries = ['Pandas', 'PostgreSQL', 'SQLite','SQLAlchemy','DuckDB']

    # Данные для построения графика
    data = np.array([[median_times[library][i] for library in libraries] for i in range(len(queries))])

    # Создание вертикальных барплотов
    fig, ax = plt.subplots(figsize=(12, 8))

    bar_width = 0.15
    bar_positions = np.arange(len(query_labels))

    for i, library in enumerate(libraries):
        ax.bar(bar_positions + i * bar_width, data[:, i], width=bar_width, label=library)

    # Добавление значений времени выполнения на график
    for i, library in enumerate(libraries):
        for j, query_label in enumerate(query_labels):
            ax.text(bar_positions[j] + i * bar_width, data[j, i] + 0.005,
                    f'{data[j, i]:.2f}', ha='center', va='bottom', fontsize=8)

    ax.set_xticks(bar_positions + (len(libraries) - 1) * bar_width / 2)
    ax.set_xticklabels(query_labels)
    ax.set_ylabel('Median Execution Time (seconds)')
    ax.legend(loc='upper left')


    plt.show()