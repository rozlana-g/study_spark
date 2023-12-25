import os
import zipfile
from src.config import settings


def combine_weather_zips(weather_dir: str) -> None:
    # Note: run in directory with only weather zip files present.

    weather_subzips = [f"{weather_dir}/{file_name}" for file_name in os.listdir(weather_dir)]
    output_zip = f"{weather_dir}/weather.zip"
    zipped_files = set()

    with zipfile.ZipFile(output_zip, mode="w") as weather_zip:
        for weather_subzip in weather_subzips:
            with zipfile.ZipFile(weather_subzip, mode="r") as weather_subfiles:
                for file in weather_subfiles.namelist():
                    if file in zipped_files:
                        continue
                    zipped_files.add(file)
                    weather_zip.writestr(file, weather_subfiles.open(file).read())

    with zipfile.ZipFile(output_zip, 'r') as zip_ref:
        # extract all files into the directory
        zip_ref.extractall(f"{weather_dir}/weather_unzipped")

    # TODO: do it in one step


if __name__ == "__main__":
    combine_weather_zips(settings.WEATHER_DIR)