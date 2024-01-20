import sys
import os

def add_parent_directory_to_path():
    current_script_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(current_script_directory)
    sys.path.insert(0, parent_directory)