import os

def combine_files():
    # Define file types to handle
    file_types = ['.py', '.yaml']
    
    # Get the directory of the script
    script_directory = os.path.dirname(os.path.abspath(__file__))
    
    # Change the working directory to one level up from the script's directory
    parent_directory = os.path.dirname(script_directory)
    os.chdir(parent_directory)
    
    # Open the combined file in write mode
    with open('combined_files.txt', 'w') as combined_file:
        # Iterate through each folder in the parent directory
        for folder_name in os.listdir(parent_directory):
            folder_path = os.path.join(parent_directory, folder_name)
            if os.path.isdir(folder_path):
                combined_file.write(f"# Contents of folder: {folder_name}\n")
                # Get the list of all .py and .yaml files in the current folder
                relevant_files = [f for f in os.listdir(folder_path) if any(f.endswith(ext) for ext in file_types)]
                for relevant_file in relevant_files:
                    relevant_file_path = os.path.join(folder_path, relevant_file)
                    print(f"Processing file: {relevant_file_path}")
                    # Open each file and read its content
                    with open(relevant_file_path, 'r') as file:
                        content = file.read()
                        # Write the content to combined_files.txt with clear comments
                        combined_file.write(f"\n# Start of file: {relevant_file}\n")
                        combined_file.write(f"# Path: {relevant_file_path}\n")
                        combined_file.write(f"# Type: {os.path.splitext(relevant_file)[1]}\n\n")
                        combined_file.write(content)
                        combined_file.write(f"\n# End of file: {relevant_file}\n")

if __name__ == "__main__":
    combine_files()
