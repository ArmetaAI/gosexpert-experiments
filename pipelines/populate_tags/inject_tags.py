import sys
from pathlib import Path
from typing import List, Tuple

sys.path.append(str(Path(__file__).parent.parent.parent))

from infrastructure.repositories import TagRepository


TAGS_DATA: List[Tuple[str, str, str, int]] = [
    ('01', 'БИМ', 'БИМ', 1),
    ('02', 'АКТИСПЫТ', 'АКТИСПЫТ', 1),
    ('03', 'АНАЛОГЗАК', 'АНАЛОГЗАК', 1),
    ('04', 'АНТИТЕРРОР', 'АНТИТЕРРОР', 1),
    ('05', 'АПР', 'АПР', 1),
    ('06', 'АСМ', 'АСМ', 1),
    ('07', 'АСР', 'АСР', 1),
    ('08', 'АУДИТОТЧЕТ', 'АУДИТОТЧЕТ', 1),
    ('09', 'БЛАГОУСТР', 'БЛАГОУСТР', 1),
    ('10', 'ГИДРО', 'ГИДРО', 1),
    ('11', 'ГОЧС', 'ГОЧС', 1),
    ('12', 'ГП', 'ГП', 1),
    ('13', 'ЗАКАРХ', 'ЗАКАРХ', 1),
    ('14', 'ЗАКСЕЙСМО', 'ЗАКСЕЙСМО', 1),
    ('15', 'ЗАКЭНЕРГО', 'ЗАКЭНЕРГО', 1),
    ('16', 'ИНЖЗАЩИТА', 'ИНЖЗАЩИТА', 1),
    ('17', 'ИНЖОБОРУДОВАНИЕ', 'ИНЖОБОРУДОВАНИЕ', 1),
    ('18', 'ИНЖСЕТИ', 'ИНЖСЕТИ', 1),
    ('19', 'МГН', 'МГН', 1),
    ('20', 'МСХРК', 'МСХРК', 1),
    ('21', 'ОРГРАБ', 'ОРГРАБ', 1),
    ('22', 'ОХРАНАТРУДА', 'ОХРАНАТРУДА', 1),
    ('23', 'ОХРПРИРОДЫ', 'ОХРПРИРОДЫ', 1),
    ('24', 'ОХРСРЕД', 'ОХРСРЕД', 1),
    ('25', 'ПБ', 'ПБ', 1),
    ('26', 'ПОТРЕБМАТ', 'ПОТРЕБМАТ', 1),
    ('27', 'ПРОМБЕЗ', 'ПРОМБЕЗ', 1),
    ('28', 'ПРОМОПАСНЫЕ', 'ПРОМОПАСНЫЕ', 1),
    ('29', 'РАЙЭКОНОМ', 'РАЙЭКОНОМ', 1),
    ('30', 'РЕШИНЖСЕТИ', 'РЕШИНЖСЕТИ', 1),
    ('31', 'РЧ', 'РЧ', 1),
    ('32', 'РЫБХОЗ', 'РЫБХОЗ', 1),
    ('33', 'РазПРИМТЕХ', 'РазПРИМТЕХ', 1),
    ('34', 'СМЕТДОК', 'СМЕТДОК', 1),
    ('35', 'СПЕЦМАТ', 'СПЕЦМАТ', 1),
    ('36', 'СР', 'СР', 1),
    ('37', 'ТРАССПЛАН', 'ТРАССПЛАН', 1),
    ('38', 'ТУАВТО', 'ТУАВТО', 1),
    ('39', 'ТУЖД', 'ТУЖД', 1),
    ('40', 'ТУПЕРЕСЕЧ', 'ТУПЕРЕСЕЧ', 1),
    ('41', 'ТУРАДИО', 'ТУРАДИО', 1),
    ('42', 'ТХМТ', 'ТХМТ', 1),
    ('43', 'ТЭП', 'ТЭП', 1),
    ('44', 'ЭПП', 'ЭПП', 1),
    ('45', 'ВОР', 'ВОР', 1),
    ('46', 'АИСГГК', 'АИСГГК', 1),
    ('47', 'ИТМГО', 'ИТМГО', 1),
    ('48', 'ЗАКЭКО', 'ЗАКЭКО', 1),
    ('49', 'ЛЕСХОЗ', 'ЛЕСХОЗ', 1),
    ('50', 'ТЕХОБСЛЕД', 'ТЕХОБСЛЕД', 1),
    ('51', 'ВОДКОМ', 'ВОДКОМ', 1),
    ('52', 'ТУГАЗ', 'ТУГАЗ', 1),
    ('53', 'ТУЛИВКАНАЛ', 'ТУЛИВКАНАЛ', 1),
    ('54', 'ЗАКСЭС', 'ЗАКСЭС', 1),
    ('55', 'СТУ', 'СТУ', 1),
    ('56', 'УТВМАТ', 'УТВМАТ', 1),
    ('57', 'СИБЯЗВА', 'СИБЯЗВА', 1),
    ('58', 'РАДОН', 'РАДОН', 1),
    ('59', 'ТУСС', 'ТУСС', 1),
    ('60', 'ДОЗИМЕТР', 'ДОЗИМЕТР', 1),
    ('61', 'ЛИЦИЗЫСК', 'ЛИЦИЗЫСК', 1),
    ('62', 'ТУТМ', 'ТУТМ', 1),
    ('63', 'ИСТФИН', 'ИСТФИН', 1),
    ('64', 'ГИР', 'ГИР', 1),
    ('65', 'АВИА', 'АВИА', 1),
    ('66', 'ГЕОДЕЗ', 'ГЕОДЕЗ', 1),
    ('67', 'АВЗУ', 'АВЗУ', 1),
    ('68', 'ЭП', 'ЭП', 1),
    ('69', 'ОЧ', 'ОЧ', 1),
    ('70', 'ГЕОЛОГ', 'ГЕОЛОГ', 1),
    ('71', 'ПИР', 'ПИР', 1),
    ('72', 'МИОРЕК', 'МИОРЕК', 1),
    ('73', 'ОПЗ', 'ОПЗ', 1),
    ('74', 'ПЗЗ', 'ПЗЗ', 1),
    ('75', 'ЗНП', 'ЗНП', 1),
    ('76', 'ЛИЦГЕН', 'ЛИЦГЕН', 1),
    ('77', 'ПОС', 'ПОС', 1),
    ('78', 'ДЕФАКТ', 'ДЕФАКТ', 1),
    ('79', 'ТУЭС', 'ТУЭС', 1),
    ('80', 'ИНФОЗАК', 'ИНФОЗАК', 1),
    ('81', 'ПП', 'ПП', 1),
    ('82', 'АПЗ', 'АПЗ', 1),
    ('83', 'ИНФОГЕН', 'ИНФОГЕН', 1),
    ('84', 'НАЧСТРОИ', 'НАЧСТРОИ', 1),
]


def inject_single_tag(code: str, name_ru: str, name_kz: str, status: int = 0) -> int:
    """
    Inject a single tag into the database.

    Args:
        code: Tag code identifier
        name_ru: Russian name
        name_kz: Kazakh name
        status: Tag status (default 0)

    Returns:
        ID of the inserted tag

    Raises:
        psycopg2.Error: If database operation fails
    """
    repo = TagRepository()

    if repo.exists(code):
        print(f"Tag with code '{code}' already exists. Skipping.")
        existing_tag = repo.get_by_code(code)
        return existing_tag.id

    tag_id = repo.insert(code, name_ru, name_kz, status)
    print(f"Inserted tag: {code} (ID: {tag_id})")
    return tag_id


def inject_tags_bulk(tags: List[Tuple[str, str, str, int]]) -> List[int]:
    """
    Inject multiple tags into the database in bulk.

    Args:
        tags: List of tuples (code, name_ru, name_kz, status)

    Returns:
        List of inserted tag IDs

    Raises:
        psycopg2.Error: If database operation fails
    """
    repo = TagRepository()

    tags_to_insert = []
    for code, name_ru, name_kz, status in tags:
        if not repo.exists(code):
            tags_to_insert.append((code, name_ru, name_kz, status))
        else:
            print(f"Tag with code '{code}' already exists. Skipping.")

    if not tags_to_insert:
        print("No new tags to insert.")
        return []

    tag_ids = repo.insert_many(tags_to_insert)
    print(f"Inserted {len(tag_ids)} tags successfully.")

    return tag_ids


def inject_predefined_tags() -> None:
    """
    Inject predefined tags from TAGS_DATA constant.

    Raises:
        psycopg2.Error: If database operation fails
    """
    print(f"Starting tag injection for {len(TAGS_DATA)} tags...")
    print("-" * 60)

    for code, name_ru, name_kz, status in TAGS_DATA:
        try:
            inject_single_tag(code, name_ru, name_kz, status)
        except Exception as e:
            print(f"Error inserting tag '{code}': {e}")

    print("-" * 60)
    print("Tag injection complete.")


def main():
    """Main entry point for tag injection script."""
    inject_predefined_tags()


if __name__ == '__main__':
    main()
