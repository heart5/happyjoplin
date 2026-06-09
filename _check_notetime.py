import pathmagic
with pathmagic.context():
    from func.jpfuncs import getapi, searchnotes, getnote
    api = getapi()
    notes = searchnotes("四件套笔记列表")
    if notes:
        note = getnote(notes[0].id)
        for attr in sorted(dir(note)):
            if not attr.startswith('_') and ('time' in attr.lower() or 'update' in attr.lower() or 'date' in attr.lower()):
                print(f"{attr}: {getattr(note, attr, 'N/A')}")
