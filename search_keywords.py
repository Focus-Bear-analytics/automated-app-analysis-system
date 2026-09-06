import pandas as pd  # library that helps us read and work with tables of data

df = pd.read_csv('data/curated/all_reviews_export.csv')  # load the full reviews file
text = (df['title'].fillna('') + ' ' + df['body'].fillna('')).str.lower()  # combine title+body into one lowercase text column

mentions_adhd = text.str.contains('adhd', regex=False)  # check which reviews contain the word "adhd"
mentions_autism = text.str.contains('autis', regex=False)  # check which reviews contain "autis" (autism/autistic)

print('Total unique reviews:', len(df))  # show how many reviews we have in total
print('Mention "adhd":', mentions_adhd.sum())  # show how many mention adhd
print('Mention "autis" (autism/autistic):', mentions_autism.sum())  # show how many mention autism
print('Mention either:', (mentions_adhd | mentions_autism).sum())  # show how many mention either one

df[mentions_adhd | mentions_autism].to_csv('data/curated/adhd_autism_keyword_search_results.csv', index=False)  # save just the matching reviews to a new file
print('Saved matches to data/curated/adhd_autism_keyword_search_results.csv')  # confirm it saved
