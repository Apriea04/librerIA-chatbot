def procesar_celda(celda):
    # Si la celda comienza y termina con comillas dobles, procesamos su contenido
    if celda.startswith('"') and celda.endswith('"'):
        # Quitamos las comillas dobles de inicio y fin
        contenido = celda[1:-1]
        # Reemplazamos comillas dobles internas por comillas simples
        contenido = contenido.replace('""', "'")
        if contenido and not contenido[0].isalnum() and contenido[0] != ' ': contenido = ' ' + contenido
        if contenido and not contenido[-1].isalnum() and contenido[-1] != ' ': contenido = contenido + ' '
        # Aseguramos que el contenido esté encerrado en comillas dobles
        return f'"{contenido}"'
    return celda

def procesar_linea(linea):
    celdas = []
    celda = ""
    dentro_comillas = False
    
    for i, char in enumerate(linea):
        # Si encontramos una comilla doble, cambiamos el estado dentro_comillas
        if char == '"':
            dentro_comillas = not dentro_comillas
            celda += char
        elif char == ',' and not dentro_comillas:
            # Si encontramos una coma y no estamos dentro de comillas, es el final de una celda
            celdas.append(procesar_celda(celda))
            celda = ""
        else:
            # Cualquier otro carácter se añade a la celda
            celda += char
    
    # Agregar la última celda procesada en la línea
    celdas.append(procesar_celda(celda))
    
    return ",".join(celdas)

def procesar_csv(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8') as outfile:
        
        for linea in infile:
            linea_procesada = procesar_linea(linea.strip())
            outfile.write(linea_procesada + '\n')

# Llamada al script
if __name__ == "__main__":
    input_file = 'books_rating.csv'  # Cambia esto al nombre de tu archivo de entrada
    output_file = 'books_rating_processed.csv'  # El archivo de salida procesado
    procesar_csv(input_file, output_file)
