from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r') as f:
        return f.read()


setup(
    name='two_method_orm',
    version='1.0.0',
    author='filthps',
    author_email='filpsixi@mail.ru',
    description='This is my first module',
    long_description=readme(),
    long_description_content_type='text/markdown',
    url='https://github.com/filthps/two_method_orm',
    packages=find_packages(),
    install_requires=[
        'Flask==3.0.2',
        'Flask-SQLAlchemy==3.1.1',
        'importlib-metadata==4.12.0',
        'pymemcache==4.0.0',
        'python-dotenv==0.20.0'
        'SQLAlchemy==2.0.28',
        'SQLAlchemy-Utils==0.38.2',
        'dill==0.3.9',
        'psycopg2==2.9.3'
    ],
    classifiers=[
        'Programming Language :: Python :: 3.9',
        'License :: GPLv3',
    ],
    keywords='orm pyqt flask flask-sqlalchemy gui ui pyside2',
    project_urls={
        'Documentation': 'https://github.com/filthps/2m'
    },
    python_requires='>=3.7',
    namespace_packages=['2m']
)
