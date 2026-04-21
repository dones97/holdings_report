files = ['modules/email_sender.py']
for f in files:
    with open(f, 'r', encoding='utf-8') as fh:
        content = fh.read()
    fixed = (content
        .replace('\u2014', '-')
        .replace('\u2013', '-')
        .replace('\u2019', "'")
        .replace('\u20b9', 'Rs.')
        .replace('\u26a0\ufe0f', 'WARNING')
        .replace('\u26a0', 'WARNING')
    )
    if fixed != content:
        with open(f, 'w', encoding='utf-8') as fh:
            fh.write(fixed)
        print('Fixed:', f)
    else:
        print('Clean:', f)
