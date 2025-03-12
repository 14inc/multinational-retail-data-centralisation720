# multinational-retail-data-centralisation720

## Project Overview
The Multinational Retail Data Centralisation (MRDC) project aims to centralize and streamline the data management processes for a multinational retail company. This project involves integrating various data sources, including legacy systems and modern databases, to provide a unified view of the company's data.

## Table of Contents
1. [Installation Instructions](#installation-instructions)
2. [Usage Instructions](#usage-instructions)
3. [Project File Structure](#file-structure-of-the-project)
4. [License Information](#license-information)

## Installation Instructions
To set up the project, follow these steps:

1. Clone the repository:
    ```sh
    git clone https://github.com/14inc/multinational-retail-data-centralisation720.git
    ```
2. Navigate to the project directory:
    ```sh
    cd multinational-retail-data-centralisation720
    ```
3. Initialize the repository and make the first commit:
    ```sh
    echo "# multinational-retail-data-centralisation720" >> README.md
    git init
    git add README.md
    git commit -m "first commit"
    git branch -M main
    git remote add origin https://github.com/14inc/multinational-retail-data-centralisation720.git
    git push -u origin main
    ```
4.  Contact dogunsade@gmail.com for the db_cred and local_db_creds YAML files to be stored in the same folder as the .py files

## Usage Instructions
To use the project, follow these steps:

1. Ensure you have the required dependencies installed:
    - Python 3.x
    - pandas
    - numpy
    - SQLAlchemy
    - psycopg2
    - boto3
    - requests
    - tabula-py
    - PyYAML

    You can install these dependencies using the following command:
    ```sh
    pip install pandas numpy sqlalchemy psycopg2-binary boto3 requests tabula-py pyyaml
    ```

2. Run the necessary scripts to integrate and process the data from various sources.

## Project File Structure
The project directory is structured as follows:

- Python source code files
- `README.md`: The main README file for the project.
- `temp.txt`: Temporary file with setup instructions and other notes.
- `sql_modelling_statements.ipynb`: A Jupyter Notebook containing the sql statements used for each task in milestone 3.

## License Information
MIT License

Copyright (c) 2025 David Adeife Ogunsade

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.