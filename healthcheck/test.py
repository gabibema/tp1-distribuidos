from multiprocessing import Process, Manager
import time

def worker(shared_list, name):
    for i in range(5):
        shared_list.append(f"{name} - {i}")
        time.sleep(1)

if __name__ == "__main__":
    with Manager() as manager:
        # Crear una lista compartida
        shared_list = manager.list()
        
        # Crear procesos
        processes = []
        for i in range(3):
            p = Process(target=worker, args=(shared_list, f"Process {i+1}"))
            processes.append(p)
            p.start()
        
        # Esperar a que los procesos terminen
        for p in processes:
            p.join()
        
        # Leer la lista compartida desde el proceso padre
        print("Contenido de la lista compartida:")
        for item in shared_list:
            print(item)
