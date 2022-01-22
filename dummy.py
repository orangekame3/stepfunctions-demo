from mimesis import Person
from mimesis.locales import Locale

person = Person(Locale.JA)
print(person.full_name())
