# The result PCollection contains one key-value element for each key in the
# input PCollections. The key of the pair will be the key from the input and
# the value will be a dictionary with two entries: 'emails' - an iterable of
# all values for the current key in the emails PCollection and 'phones': an
# iterable of all values for the current key in the phones PCollection.

emails_list = [
    ('amy', 'amy@example.com'),
    ('carl', 'carl@example.com'),
    ('julia', 'julia@example.com'),
    ('carl', 'carl@email.com'),
]
phones_list = [
    ('amy', '111-222-3333'),
    ('james', '222-333-4444'),
    ('amy', '333-444-5555'),
    ('carl', '444-555-6666'),
]

emails = p | 'CreateEmails' >> beam.Create(emails_list)
phones = p | 'CreatePhones' >> beam.Create(phones_list)

results = [
    (
        'amy',
        {
            'emails': ['amy@example.com'],
            'phones': ['111-222-3333', '333-444-5555']
        }),
    (
        'carl',
        {
            'emails': ['carl@email.com', 'carl@example.com'],
            'phones': ['444-555-6666']
        }),
    ('james', {
        'emails': [], 'phones': ['222-333-4444']
    }),
    ('julia', {
        'emails': ['julia@example.com'], 'phones': []
    }),
]

results = ({'emails': emails, 'phones': phones} | beam.CoGroupByKey())

def join_info(name_info):
  (name, info) = name_info
  return '%s; %s; %s' %\
      (name, sorted(info['emails']), sorted(info['phones']))

contact_lines = results | beam.Map(join_info)