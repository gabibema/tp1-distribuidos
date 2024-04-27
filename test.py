def format_output(date_str, categories_str):
    # Extraer solo el año usando split y maxsplit
    year = date_str.split('-', maxsplit=1)[0]
    year_dots = '.'.join(year)  # Separa los caracteres del año con puntos
    
    # Procesar la cadena de categorías, asumiendo que está en formato de lista como string
    categories = categories_str.strip("[]")
    categories = categories.replace("'", "").replace(" & ", ".")
    categories_list = categories.split(", ")
    categories_dots = '.'.join(categories_list)
    
    # Concatenar las partes con el formato deseado
    result = f"{categories_dots}.{year_dots}"
    return result

# Ejemplo de uso:
date1 = '2005-01-01'
categories1 = "['Comics, Drama & Graphic Novels']"
output1 = format_output(date1, categories1)
print(output1)

date2 = '1992'
categories2 = "['Comics']"
output2 = format_output(date2, categories2)
print(output2)
