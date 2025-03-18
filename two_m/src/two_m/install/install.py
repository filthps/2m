"""
Copyright (C) 2025 Литовченко Виктор Иванович (filthps)
Файл для развёртывания пакетов в рабочем каталоге целевого проекта.
Данный модуль следует запускать посредством CLI:
1) Активируем виртуальное окружение, если таковое в вашем проекте предусмотрено, иначе используем общесистемный подход
2) Переходим в рабочий каталог проекта
    Например: cd my_project
3) import two_m
3) Запускаем данный модуль через python
    Например: c:/my_project/my_app> python3 two_m.install
Теперь можно писать свои модели и хранимые процедуры в models и procedures соответственно
"""
import os
import time
import subprocess
import sys
import shutil
import importlib
import typing
from subprocess import CompletedProcess


def get_app_path():
    return importlib.import_module(MODULE_NAME).__file__


def read_requirements() -> list[str]:
    file_ = open(os.path.join(MODULE_URL, REQUIREMENTS_TXT_PATH))
    t = file_.read()
    file_.close()
    return t.split("\r\n")


MODULE_NAME = 'two_m'
MODULE_URL = os.path.abspath(get_app_path())
TEMPLATES_ROOT = '/templates/'  # Пакет с модулями, которые должны распаковаться в пользовательское приложение
REQUIREMENTS_TXT_PATH = 'requirements.txt'
TEMPLATES_URL = os.path.join(MODULE_URL, TEMPLATES_ROOT)
REQUIRED_PYTHON_VERSION = '3.7'
INSTALLATION_PATH = os.path.abspath(os.getcwd())
REQUIREMENTS_LIST = read_requirements()
MAX_RETRIES_CHECK_REQUIREMENTS = 10
DELAY_SEC_RETRY_CHECK_REQUIREMENTS = 5 * 1000


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
        if data.returncode < 0:
            return False
        if not data.returncode == 0:
            print(f"Повторная проверка наличия requirements через {DELAY_SEC_RETRY_CHECK_REQUIREMENTS // 1000} секунд")
            print(f"Попытка {counter} из {MAX_RETRIES_CHECK_REQUIREMENTS}")
            if counter == MAX_RETRIES_CHECK_REQUIREMENTS:
                return False
            time.sleep(DELAY_SEC_RETRY_CHECK_REQUIREMENTS)
            return is_success(data, counter=counter + 1)
        return True

    def check_not_installed_package_names(names: typing.Iterable):
        return frozenset(map(lambda x: x.split('==')[0], REQUIREMENTS_LIST)) - \
               frozenset(map(lambda x: x.split('==')[0], names))

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
    shutil.copytree(TEMPLATES_URL, os.getcwd(),
                    ignore=shutil.ignore_patterns('*.pyc', 'tmp*'))


if __name__ == '__main__':
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
