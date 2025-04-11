"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Файл для развёртывания пакетов в рабочем каталоге целевого проекта.
"""
import os
import time
import subprocess
import sys
import shutil
import importlib
import typing
from subprocess import CompletedProcess


def main():
    if not check_items_is_exist(MODULE_URL):
        raise RuntimeError('Не удалось инициализировать установку')
    if not check_items_is_exist(TEMPLATES_URL):
        raise RuntimeError('Не найдены пакеты для распаковки')
    check_python_version()
    print("1/4 -- OK check Python Ver")
    install_requirements()
    print("2/4 -- OK install requirements")
    check_requirements()
    print("3/4 -- OK check exists requirements")
    copy_items()
    print("4/4 -- OK copy files")
    print("Success")


def get_app_path():
    def remove_init_py(full_path: str):
        arr = full_path.split(os.path.sep)
        if "__init__.py" in arr:
            arr.remove("__init__.py")
        return f"{os.path.sep}".join(arr)
    return remove_init_py(importlib.import_module(MODULE_NAME).__file__)


def read_requirements() -> list[str]:
    def filter_empty_line(elems):
        if "" in elems:
            elems.remove("")
        return elems
    file_ = open(os.path.join(MODULE_URL, REQUIREMENTS_TXT_PATH))
    t = file_.read()
    file_.close()
    return filter_empty_line(t.split("\n"))


MODULE_NAME = "two_m_root"
PACKAGE_NAME = 'two_m'  # Название пакета, который появится у юзера в рабочем каталоге
MODULE_URL = os.path.abspath(get_app_path())
TEMPLATES_ROOT = '/templates/'  # Пакет с модулями, которые должны распаковаться в пользовательское приложение
REQUIREMENTS_TXT_PATH = 'install/requirements.txt'
TEMPLATES_URL = f"{MODULE_URL}{os.path.sep}{TEMPLATES_ROOT}{os.path.sep}"
REQUIRED_PYTHON_VERSION = '3.8'
INSTALLATION_PATH = os.path.abspath(os.getcwd())
REQUIREMENTS_LIST = read_requirements()
MAX_RETRIES_CHECK_REQUIREMENTS = 10
DELAY_SEC_RETRY_CHECK_REQUIREMENTS = 2 * 1000


def check_items_is_exist(path):
    return os.path.exists(path)


def check_python_version():
    if any(filter(lambda x: x[0] < int(x[1]),
                  zip(sys.version_info[:2], REQUIRED_PYTHON_VERSION.split(".")))):
        raise RuntimeError('Версия Python ниже минимальной')


def install_requirements():
    subprocess.run(['pip', 'install', *REQUIREMENTS_LIST])


def check_requirements():
    def is_success(data: CompletedProcess, counter=0) -> bool:
        if data.returncode < 0:  # Процесс завершился ошибкой
            return False
        if data.returncode == 0:  # Процесс завершился ОК
            return True
        print(f"Повторная проверка наличия requirements через {DELAY_SEC_RETRY_CHECK_REQUIREMENTS // 1000} секунд")
        print(f"Попытка {counter} из {MAX_RETRIES_CHECK_REQUIREMENTS}")
        if counter == MAX_RETRIES_CHECK_REQUIREMENTS:
            return False
        time.sleep(DELAY_SEC_RETRY_CHECK_REQUIREMENTS)
        return is_success(data, counter=counter + 1)

    def check_not_installed_package_names(exists_package_names: typing.Iterable):
        return frozenset(map(lambda x: x.split('==')[0], REQUIREMENTS_LIST)).issubset(
               frozenset(map(lambda x: x.split('==')[0], exists_package_names)))

    def check_packages_with_out_date_version():
        def is_valid_version(version: str, required_version: str):
            for fact, required in zip(map(lambda v: v.split("."), version), map(lambda v: v.split("."), required_version)):
                if fact < required:
                    return False
                return True
        required_d = {}
        for str_ in REQUIREMENTS_LIST:
            index = str_.index("==")
            name, version_str = str_[:index], str_[index+2:]
            required_d.update({name: version_str})
        for package_name in package_items:
            if not package_name:
                continue
            n, v = package_name.split("==")
            if n in required_d:
                if not is_valid_version(v, required_d[n]):
                    yield f"{n}=={v} < {required_d[n]}"
    proc: CompletedProcess = subprocess.run(['pip', 'freeze'], capture_output=True)
    if is_success(proc):
        row = str(proc.stdout, 'utf-8')
        package_items = row.split('\r\n')
        not_installed_packages = check_not_installed_package_names(package_items)
        br = "\n"
        if not_installed_packages:
            raise Exception(f'Следующие пакеты отсутствуют: {br}{br.join(package_items)}')
        invalid_version_packages = list(check_packages_with_out_date_version())
        if invalid_version_packages:
            raise Exception(f'Следующие пакеты не соответствуют по версиям: {br}{br.join(invalid_version_packages)}')
        return
    raise RuntimeError('Не удалось выполнить установку. Проверка requirements не удалась.')


def copy_items():
    target_path = f"{os.getcwd()}{os.path.sep}{PACKAGE_NAME}"
    shutil.copytree(TEMPLATES_URL, f"{target_path}{os.path.sep}",
                    ignore=shutil.ignore_patterns('*.pyc', 'tmp*'))


if __name__ == '__main__':
    main()
