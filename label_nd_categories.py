import sys
sys.path.insert(0, '.')  # lets Python find the scrapers folder from here

import pandas as pd  # library that helps us read and work with tables of data
from scrapers.reviews_pipeline import get_nd_categories  # bring in our ADHD/autism-labeling function

df = pd.read_csv('data/curated/all_reviews_export.csv')  # load the full reviews file
df['title'] = df['title'].fillna('')  # replace any missing titles with empty text
df['body'] = df['body'].fillna('')  # replace any missing bodies with empty text

df['nd_categories'] = df.apply(lambda r: get_nd_categories(r['title'], r['body']), axis=1)  # label every review with its category(s)

adhd_count = df['nd_categories'].apply(lambda c: 'adhd' in c).sum()  # count how many reviews are labeled ADHD
autism_count = df['nd_categories'].apply(lambda c: 'autism' in c).sum()  # count how many are labeled autism
both_count = df['nd_categories'].apply(lambda c: 'adhd' in c and 'autism' in c).sum()  # count how many are labeled both
other_nd_count = df['nd_categories'].apply(lambda c: 'other_nd' in c).sum()  # count how many are labeled "other neurodivergent"
any_nd = df['nd_categories'].apply(lambda c: len(c) > 0).sum()  # count how many have at least one label

print('Total reviews:', len(df))  # show the total number of reviews
print('ADHD-labeled:', adhd_count)  # show the ADHD count
print('Autism-labeled:', autism_count)  # show the autism count
print('Both ADHD and autism:', both_count)  # show how many overlap both
print('Other ND (dyslexia/dyspraxia/tourette\'s/sensory processing/executive function):', other_nd_count)  # show the other-ND count
print('Any neurodivergent label at all:', any_nd)  # show the combined total

labeled_reviews = df[df['nd_categories'].apply(len) > 0]  # keep only the reviews that got at least one label
labeled_reviews.to_csv('data/curated/nd_labeled_reviews.csv', index=False)  # save just those labeled reviews to a new file

print()
print('Saved labeled reviews to data/curated/nd_labeled_reviews.csv')  # confirm it saved
