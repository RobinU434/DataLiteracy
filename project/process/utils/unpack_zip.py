import zipfile


def unzip(*files: str):
    for file in files:
        target_directory = file.split(".")[0]
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(target_directory)
