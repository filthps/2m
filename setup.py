from setuptools import setup, find_packages


def readme():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()


if __name__ == "__main__":
    setup(
        name='2m',
        version='1.1',
        author='Victor Litovchenko',
        author_email='filpsixi@mail.ru',
        description='This is my first module',
        long_description=readme(),
        long_description_content_type='text/markdown',
        url='https://github.com/filthps/2m',
        packages=find_packages(),
        install_requires=[
            'Flask==3.0.2',
            'Flask-SQLAlchemy==3.1.1',
            'importlib-metadata==8.6.1',
            'pymemcache==4.0.0',
            'python-dotenv==0.20.0',
            'SQLAlchemy==2.0.28',
            'SQLAlchemy-Utils==0.38.2',
            'dill==0.3.9'
        ],
        classifiers=[
            'Programming Language :: Python :: 3.8'
        ],
        keywords='orm pyqt flask flask-sqlalchemy gui ui pyside2',
        project_urls={
            'Documentation': 'https://github.com/filthps/2m'
        },
        python_requires='>=3.8',
    )
