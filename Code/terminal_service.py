import matplotlib
matplotlib.use('TkAgg')  # o 'Qt5Agg'

import matplotlib.pyplot as plt
import json

class terminal_service:
    def __init__(self):
        self.dict_pollution = dict()
        self.dict_wellness = dict()
        self.first = True
        self.data_points_pollution = {}  # Diccionario para almacenar los datos por ID
        self.data_points_wellness = {}  # Diccionario para almacenar los datos por ID
        self.colors = ['blue', 'green', 'red', 'cyan', 'magenta', 'yellow']  # Lista de colores para las líneas
        self.color_index = 0  # Índice para recorrer la lista de colores
        self.fig, self.ax_pollution, self.ax_wellness = None, None, None

    def send_results(self, pollution_data, wellness_data, id_terminal):
        if self.first:
            self.fig, (self.ax_pollution,self.ax_wellness) = plt.subplots(2, 1, figsize=(8, 6))

        for id, values in pollution_data.items():
            if id not in self.data_points_pollution:
                self.data_points_pollution[id] = {'timestamps': [], 'coefficients': []}
                self.first = True
            timestamp = values['timestamp']
            coefficient = values['coefficient']

            if timestamp in self.data_points_pollution[id]['timestamps']:
                index = self.data_points_pollution[id]['timestamps'].index(timestamp)
                self.data_points_pollution[id]['coefficients'][index] = coefficient
                continue  # Si el timestamp ya ha sido registrado para este ID, pasar al siguiente dato

            self.data_points_pollution[id]['timestamps'].append(timestamp)  # Almacenar el timestamp
            self.data_points_pollution[id]['coefficients'].append(coefficient)  # Almacenar el coeficiente
        if self.first:
            self.ax_pollution.clear()
        for id, data in self.data_points_pollution.items():
            timestamps = data['timestamps']
            color = self.get_color(id)  # Obtener el color correspondiente al ID
            self.ax_pollution.plot(timestamps, data['coefficients'], marker='o',
                                   label=f'ID {id}',
                                   color=color)  # Graficar los puntos y asignar etiqueta y color a cada línea

        if self.first:
            self.ax_pollution.set_title(f'Pollution, Terminal:{id_terminal}', loc="left",
                                        fontdict={'fontsize': 14, 'fontweight': 'bold', 'color': 'tab:blue'})
            self.ax_pollution.legend()  # Mostrar leyenda con las etiquetas de las líneas

        for id, values in wellness_data.items():
            if id not in self.data_points_wellness:
                self.data_points_wellness[id] = {'timestamps': [], 'coefficients': []}
                self.first = True

            timestamp = values['timestamp']
            coefficient = values['coefficient']

            if timestamp in self.data_points_wellness[id]['timestamps']:
                index = self.data_points_wellness[id]['timestamps'].index(timestamp)
                self.data_points_wellness[id]['coefficients'][index] = coefficient
                continue  # Si el timestamp ya ha sido registrado para este ID, pasar al siguiente dato

            self.data_points_wellness[id]['timestamps'].append(timestamp)  # Almacenar el timestamp
            self.data_points_wellness[id]['coefficients'].append(coefficient)  # Almacenar el coeficiente
        if self.first:
            self.ax_wellness.clear()
        for id, data in self.data_points_wellness.items():
            timestamps = data['timestamps']
            color = self.get_color(id)  # Obtener el color correspondiente al ID
            self.ax_wellness.plot(timestamps, data['coefficients'], marker='o',
                                  label=f'ID {id}',
                                  color=color)  # Graficar los puntos y asignar etiqueta y color a cada línea

        if self.first:
            self.ax_wellness.set_title(f'Wellness, Terminal:{id_terminal}', loc="left",
                                       fontdict={'fontsize': 14, 'fontweight': 'bold', 'color': 'tab:blue'})
            self.ax_wellness.legend()  # Mostrar leyenda con las etiquetas de las líneas

            self.first = False

        plt.tight_layout()  # Ajustar el espacio
        plt.draw()
        plt.pause(2)

    def get_color(self, id):
        if id in self.dict_pollution:
            return self.dict_pollution[id]  # Si el ID ya tiene asignado un color, devolverlo
        else:
            color = self.colors[self.color_index]  # Obtener el próximo color en la lista
            self.dict_pollution[id] = color  # Asignar el color al ID en el diccionario
            self.color_index = (self.color_index + 1) % len(
                self.colors)  # Incrementar el índice para el siguiente color
            return color

terminal_service = terminal_service()