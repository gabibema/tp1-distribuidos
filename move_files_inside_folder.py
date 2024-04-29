import os
import shutil

def move_and_rename_files(directory):
    """Move each .py file in the specified directory into a subdirectory named after the file and rename it to main.py."""
    for filename in os.listdir(directory):

        if filename.endswith('.py'):
            subdir_name = os.path.splitext(filename)[0]
            subdir_path = os.path.join(directory, subdir_name)
            
            # Crear el subdirectorio si no existe
            os.makedirs(subdir_path, exist_ok=True)
            

            original_file_path = os.path.join(directory, filename)
            new_file_path = os.path.join(subdir_path, 'test.py')
            
            # Mover y renombrar el archivo al subdirectorio
            shutil.move(original_file_path, new_file_path)
            print(f"Moved {filename} to {new_file_path}")

if __name__ == "__main__":

    directory_to_process = './workers'
    move_and_rename_files(directory_to_process)
