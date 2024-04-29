import os
import shutil

def move_and_rename_files(directory, mapping):
    """Move each .py file in the specified directory into a subdirectory based on a mapping or named after the file, then rename it to main.py."""

    for filename in os.listdir(directory):
        if filename.endswith('.py'):
            file_base_name = os.path.splitext(filename)[0]
            
            subdir_name = mapping.get(file_base_name, file_base_name)
            subdir_path = os.path.join(directory, subdir_name)
            
            os.makedirs(subdir_path, exist_ok=True)

            original_file_path = os.path.join(directory, filename)
            new_file_path = os.path.join(subdir_path, 'main.py')
            
            shutil.move(original_file_path, new_file_path)
            print(f"Moved {filename} to {new_file_path}")

if __name__ == "__main__":
    directory_to_process = './workers'
    file_to_subdir_mapping = {
        'top_10': 'top_fiction_books',
        'nlp': 'nlp_worker',
        'title_filter': 'computer_books'
    }
    move_and_rename_files(directory_to_process, file_to_subdir_mapping)
