import sys
sys.path.insert(0, '.')  # lets Python find the scrapers folder from here
from scrapers.reviews_pipeline import is_special_review, get_nd_categories  # bring in the functions we want to test

# A list of test reviews, each paired with what we expect the answer to be
cases = [
    ('', 'As someone with ADHD this app really helps me focus', ['adhd']),
    ('', 'I have ADD and this timer helps a lot', ['adhd']),
    ('', 'My autism makes routines hard, this app helps', ['autism']),
    ('', 'I am autistic and have ADHD, this app is great for both', ['adhd', 'autism']),
    ('', 'This app has great sensory processing support for my kid', ['other_nd']),
    ('', 'Please add dark mode', []),
    ('', "Diagnosed with Asperger's last year, this app helps a ton", ['autism']),
]

# Go through each test one at a time
for title, body, expected in cases:
    got = get_nd_categories(title, body)  # run our function on this test review
    status = "PASS" if got == expected else "FAIL"  # check if the result matches what we expected
    print(f'{status} | got={got} expected={expected} | {body}')  # show the result on screen