from src.storage import Storage
storage = Storage()
download = storage.download_core('/my-file.parquet')
print(download[-1].text)