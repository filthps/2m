class ModelsConfigurationError(Exception):
    def __init__(self, text=""):
        super().__init__(text or "Ошибка в models.py. Конфигурация чисто кастомная! \
                                Стандартные Models от Flask не годятся")


class ORMException(Exception):
    def __init__(self, text="Ошибка уровня модуля orm"):
        super().__init__(text)


class PointerException(ORMException):
    def __init__(self, text="Ошибка экземпляра Pointer"):
        super().__init__(text)


class PointerEmptyWrapper(PointerException):
    def __init__(self, val="Контейнер wrappers пуст"):
        super().__init__(val)


class PointerWrapperTypeError(PointerException):
    def __init__(self, value=""):
        super().__init__(text=value)


class PointerWrapperLengthError(PointerException):
    def __init__(self, text="Длина элементов в wrap_items больше не соответствует длине результатов"):
        super().__init__(text)


class PointerRepeatedWrapper(PointerException):
    def __init__(self, t="В конфигурации данного Pointer есть 1 или более повторяющееся значение в элементах обёртки"):
        super().__init__(text=t)


class ORMInitializationError(ORMException):
    def __init__(self, text="Неправильная настройка модуля"):
        super().__init__(text)


class ORMExternalDataError(ORMException):
    def __init__(self, text="Данные в кеше непригоды для использования в рамках текущей конфигурации. \
                            Требуется сброс"):
        super.__init__(text)


class NodeError(ORMException):
    def __init__(self, text="Ошибка ноды в очереди"):
        super().__init__(text)


class NodePrimaryKeyError(NodeError):
    def __init__(self,
                 text="Неверно указан первичный ключ, который, как и его значение должен содержаться в словаре value"):
        super().__init__(text)


class NodeDMLTypeError(NodeError):
    def __init__(self, text="Любая нода, будь то insert, update или delete, \
                            должна иметь в значении поле первичного ключа (для базы данных) со значением!"):
        super().__init__(text)


class NodeEmptyData(NodeError):
    def __init__(self, text="Нода не содержит полей с данными"):
        super().__init__(text)


class NodeAttributeError(NodeError):
    def __init__(self, text="Ошибка значиния атрибута в ноде"):
        super().__init__(text)


class NodeColumnError(NodeError):
    def __init__(self, text="", field_name="", model_name=""):
        if text:
            super().__init__(text)
            return
        super().__init__(f"Столбец {field_name} не найден в таблице {model_name if model_name else ''}"
                         if field_name else text)


class NodeColumnValueError(NodeColumnError):
    def __init__(self, value=None, text="Ошибка значения столбца в ноде"):
        super().__init__(f"Значение {value} не является корректным" if value else text)


class InvalidModel(NodeError):
    def __init__(self, text="Нужен класс CustomModel, наследованный от flask-sqlalchemy.Model. Смотри models.py"):
        super().__init__(text)


class QueueError(ORMException):
    def __init__(self, text="Ощибка в контейнере(очереди) нод"):
        super().__init__(text)


class DoesNotExists(QueueError):
    def __init__(self, text="Нода не найдена"):
        super().__init__(text)


class JoinedResultError(ORMException):
    def __init__(self, message="Исключение на уровне экземпляра класса JoinedORMItem"):
        super().__init__(message)


class JoinedItemPointerError(JoinedResultError):
    def __init__(self, message="Ошибка Pointer. \
                               При создании экземпляра передан недействительный тип результата: \
                               Pointer работает только с Result и JoinSelectResult"):
        super().__init__(message)


class WrapperError(JoinedItemPointerError):
    def __init__(self, msg="В качестве элементов-указателей на содержимое принимается список строк"):
        super().__init__(msg)
