import subprocess
required_packages = ["mysql-connector-python", "uuid", "json", "pandas", "ast"]


def install_packages(packages):
    for package in packages:
        subprocess.call(["pip", "install", package])


install_packages(required_packages)
