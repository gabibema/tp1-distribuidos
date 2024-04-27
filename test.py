import csv
from io import StringIO

# Encabezados predefinidos
headers = ["Title", "description", "authors", "image", "previewLink", "publisher", "publishedDate", "infoLink", "categories", "ratingsCount"]

# Simulando un archivo CSV como string
data = """Its Only Art If Its Well Hung!,,['Julie Strain'],http://books.google.com/books/content?id=DykPAAAACAAJ&printsec=frontcover&img=1&zoom=1&source=gbs_api,http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&cd=1&source=gbs_api,,1996,http://books.google.nl/books?id=DykPAAAACAAJ&dq=Its+Only+Art+If+Its+Well+Hung!&hl=&source=gbs_api,['Comics & Graphic Novels']
"Dr. Seuss: American Icon","Philip Nel takes a fascinating look into the key aspects of Seuss's career - his poetry, politics, art, marketing, and place in the popular imagination."" ""Nel argues convincingly that Dr. Seuss is one of the most influential poets in America. His nonsense verse, like that of Lewis Carroll and Edward Lear, has changed language itself, giving us new words like ""nerd."" And Seuss's famously loopy artistic style - what Nel terms an ""energetic cartoon surrealism"" - has been equally important, inspiring artists like filmmaker Tim Burton and illustrator Lane Smith. --from back cover",['Philip Nel'],http://books.google.com/books/content?id=IjvHQsCn_pgC&printsec=frontcover&img=1&zoom=1&edge=curl&source=gbs_api,http://books.google.nl/books?id=IjvHQsCn_pgC&printsec=frontcover&dq=Dr.+Seuss:+American+Icon&hl=&cd=1&source=gbs_api,A&C Black,2005-01-01,http://books.google.nl/books?id=IjvHQsCn_pgC&dq=Dr.+Seuss:+American+Icon&hl=&source=gbs_api,['Biography & Autobiography']
""".split("\n")

for row in data:
    csv_file = StringIO(row)

# Crear DictReader indicando los encabezados y el objeto de archivo
    reader = csv.DictReader(csv_file, fieldnames=headers)
    for row in reader:
        print(row)
