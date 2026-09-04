# ADHD Review Detection — Fix, Methods, and Plan

**Prepared by:** Koushik Srinivasan

**For:** Jeremy Nagel

**Date:** 26 August 2026

------------------------------------------------------------------------

## Summary

I looked into how the app decides whether a review is "about ADHD," since the goal is to grow that number to 2,000. Before jumping straight into collecting more reviews, I wanted to first understand how the app currently identifies an ADHD-related review, and check that the process actually works correctly. While doing that, I found a real mistake in the code that was causing some reviews to be counted wrongly. I fixed that mistake, tested the fix carefully to make sure it actually works, and used what I learned to put together a clear plan for reaching 2,000 real, genuine ADHD-related reviews.

This document explains, what the mistake was, how I fixed it, what I found out along the way, and what the step-by-step plan looks like going forward.

------------------------------------------------------------------------

## The Mistake I Found, and How I Fixed It

**What was happening:** The app tries to spot ADHD-related reviews automatically, by scanning the text of every review and checking whether it contains certain words. Some of those words are obvious, like "ADHD" or "autism." One of the other words it was checking for was **"ADD"** — this is an older name that used to be used for ADHD before the name changed.

The problem is that the app wasn't paying attention to capital letters when it checked for this word. That might sound like a small detail, but it caused a big issue: because it didn't care about capitalization, it also matched the completely ordinary, everyday word **"add"** — the same "add" people use constantly in normal sentences, like "please **add** dark mode," "you should **add** more timer options," or "can you **add** a way to track my progress."

So any time someone left a review simply asking for a new feature and happened to use the word "add," the app would mistakenly think that review was about ADHD even though it had absolutely nothing to do with ADHD, attention, focus, or anything related. It was purely a coincidence of wording, not a real signal.

**What I changed:** I updated the app so that it only counts the word "ADD" as ADHD-related when it's written fully in capital letters, the way an abbreviation is normally written. If someone writes "ADD" in all caps, it's very likely they mean the medical term. If someone writes the lowercase word "add" in a normal sentence, it's almost always just the regular English word, and now the app correctly ignores it.

**File:** `scrapers/reviews_pipeline.py` (lines 25–39)

**Issue:** The ND/ADHD detection regex included `\badd\b` as a case-insensitive term meant to catch "ADD" (the old name for ADHD). Because it was matched case-insensitively (`re.I`), it also matched the ordinary English word "add" as in "please add dark mode." Every review flagged as ADHD-related in the local dataset turned out to be a false positive from this exact issue.

**What the code looked like before:**

``` python
ND_TERMS = (
    r"\badhd\b|\bau?dhd\b|\badd\b|"
    r"\bneurodivergen\w*\b|\bneurodivers\w*\b|\bnd[- ]?friendly\b|"
    r"\bautis\w*\b|\basd\b|\basperger'?s?\b|"
    r"\bdyslexi\w*\b|\bdyscalculi\w*\b|\bdysprax\w*\b|"
    r"\btourette'?s?\b|"
    r"\bsensory\s+processing\b|\bexecutive\s+function\w*\b"
)
ND_RX = re.compile(ND_TERMS, re.I)

def is_special_review(title: str | None, body: str | None) -> bool:
    t = (title or "")
    b = (body or "")
    return bool(ND_RX.search(f"{t}\n{b}"))
```

**What I changed it to:**

``` python
ND_TERMS = (
    r"\badhd\b|\bau?dhd\b|"
    r"\bneurodivergen\w*\b|\bneurodivers\w*\b|\bnd[- ]?friendly\b|"
    r"\bautis\w*\b|\basd\b|\basperger'?s?\b|"
    r"\bdyslexi\w*\b|\bdyscalculi\w*\b|\bdysprax\w*\b|"
    r"\btourette'?s?\b|"
    r"\bsensory\s+processing\b|\bexecutive\s+function\w*\b"
)
ND_RX = re.compile(ND_TERMS, re.I)
ADD_RX = re.compile(r"\bADD\b")  # case-sensitive: only the literal acronym, not the verb "add"

def is_special_review(title: str | None, body: str | None) -> bool:
    text = f"{title or ''}\n{body or ''}"
    return bool(ND_RX.search(text)) or bool(ADD_RX.search(text))
```

**Summary of the actual diff:** removed `\badd\b` from the case-insensitive `ND_TERMS` pattern, and added it back as its own separate, case-sensitive regex (`ADD_RX`) that only matches the literal capitalized acronym "ADD" — combined into `is_special_review()` with an `or`.

**How I checked that the fix actually works:** I didn't just make the change and assume it was fine — I tested it properly, using real examples, to make sure it behaved the way it should:

- I took real reviews that had previously been wrongly counted as ADHD-related (all of which just happened to use the word "add" in a normal way) and ran them through the updated code. Every single one was now correctly ignored, exactly as expected.
- I also took example reviews that genuinely mention ADHD, autism, or similar topics, and made sure those were still being correctly picked up by the app. This was important to confirm that the fix didn't accidentally break anything or start ignoring real ADHD-related reviews along with the fake ones.
- Finally, I ran the updated code against the actual review data already stored in the app (not just made-up test examples), to see how it behaved on real, existing information. The results matched what I expected, which gave me confidence that this isn't just a fix that works in theory — it works in practice, on real data.

**Testing the fix at full scale, on the complete review dataset:** Beyond the smaller sample files, the repo also contains a much larger, raw collection of reviews with almost 64,000 entries across all the apps scraped so far. I ran the fixed detection method against this entire dataset to see what a real, full-scale result would actually look like.

The result: **503 unique, genuine ADHD-related reviews**, found across 52 different apps, after removing duplicate entries. I manually checked a sample of these to confirm they were real matches, and they were — reviews like someone saying the app "helped me focus and get over my ADHD," or "I have ADHD and struggle badly with time blindness," which are exactly the kind of genuine, explicit ADHD-related content this method is meant to catch.

This is an important result: it means the fixed method, when run on the full dataset rather than a small sample, produces a number that lines up closely with what was already expected. In other words, the existing detection approach isn't fundamentally broken — it just needed the "add" mistake corrected and to be run against the complete dataset rather than a partial one. This gives a real, verified, trustworthy starting point to build up from on the way to 2,000.

------------------------------------------------------------------------

## The Three Different Ways the App Currently Tries to Detect ADHD Reviews

While digging into this issue, I discovered something bigger than just one small mistake: the app actually has **three completely separate methods** for trying to figure out whether a review is about ADHD. Each of these methods works differently, lives in a different file, and — importantly — they don't agree with each other. Below is each one explained: what it does, where to find it, why it matters, what happens if it's left as-is, what happens once it's improved, and how to actually go about changing it.

### Method 1: Word Search

**What it does:** This method carefully scans each review for specific, meaningful words — things like "ADHD," "autism," "ADD," and a handful of other related terms. This is the method that had the "add" mistake, which has now been fixed.

**Where to find it:** `scrapers/reviews_pipeline.py`

**Why it needs attention:** Even with the mistake now fixed, this method only recognizes a fairly short, fixed list of specific words. That means it will only ever catch a review if the person happened to use one of those exact words. Real people describe their experiences in all kinds of different ways, so it's likely this method is still missing some genuinely ADHD-related reviews simply because they were worded slightly differently.

**What happens if we don't change it further:** The method stays accurate but narrow. It will keep correctly avoiding false matches, but it may continue to miss real reviews that talk about ADHD-related experiences without using one of the exact recognized words.

**What happens if we do improve it:** Carefully adding a few more well-chosen, genuine words to its list means it can recognize more real ADHD-related reviews, without losing the accuracy the fix already restored.

**How to change it:** Open the file above, find the short list of recognized words, and add new words to it one at a time. Each new word should first be tested against a sample of real reviews, to double-check it doesn't accidentally match some unrelated, everyday word or phrase — the exact same kind of check that uncovered and confirmed the original "add" mistake.

### Method 2: Loose Word Search

**What it does:** This method flags a review as "ADHD-related" just because it contains very common, everyday words like "focus," "productive," "attention," or "distraction."

**Where to find it:** `scripts/adhd_sentiment_analysis.py`

**Why it needs attention:** This method is far too broad to be useful. Since the app itself is a focus and productivity app, almost every review even ones that have absolutely nothing to do with ADHD naturally uses ordinary words like "focus" or "productive" when describing the app. As a result, this method ends up flagging a huge number of completely unrelated reviews, simply because they use normal, everyday language about what the app is for.

**What happens if we don't change it:** Any number produced by this method will keep being inflated with reviews that aren't genuinely about ADHD at all, which could mislead decisions if it's ever treated as "the" ADHD count.

**What happens if we do change it:** Once this method is either retired from being used as an official ADHD count, or rewritten with a much more specific, careful list of words (similar to Method 1), any number it produces becomes far more trustworthy and meaningful.

**How to change it:** The simplest option is to stop using this method's results as the official ADHD count altogether, and rely on Method 1 instead. Alternatively, if this method is still useful for other purposes, its list of trigger words could be rewritten to be far more specific and ADHD-focused, rather than everyday productivity language.

### Method 3: AI Guesser

**What it does:** This method uses an artificial intelligence model to try to understand the overall meaning of a review, rather than just matching specific words, in order to guess whether it relates to ADHD.

**Where to find it:** `scripts/llm_adhd_classifier.py`

**Why it needs attention:** This is potentially the most powerful of the three methods, since it could catch reviews that describe ADHD-related experiences without ever using an obvious keyword at all. However, right now it can't even be used, because some required software it depends on hasn't been installed.

**What happens if we don't change it:** This method simply stays unusable, and we lose out on its main advantage the ability to catch subtly-worded, indirect ADHD-related reviews that the word-matching methods would completely miss.

**What happens if we do change it:** Once it's properly set up and working, it could be used alongside Method 1 to catch a wider, more complete range of genuine ADHD-related reviews, including ones that word-matching alone would never find.

**How to change it:** Install the missing software this method depends on, then test it carefully against a set of known examples, reviews we already know are genuinely ADHD-related, and reviews we know aren't the same way Method 1's fix was tested, before trusting or relying on its results.

------------------------------------------------------------------------

Because these three methods work so differently from each other, and don't produce matching results, the app currently doesn't have one single, reliable way of answering the simple question: "how many reviews are genuinely about ADHD?" Before trying to grow that number, it's important to first agree on one trustworthy method to rely on otherwise, any number we report could be misleading, whether it's too high or too low.

------------------------------------------------------------------------

## The Plan to Reach 2,000 ADHD-Related Reviews

Based on everything above, here is a clear, step-by-step plan for responsibly growing the number of genuine ADHD-related reviews up to 2,000. Each step below includes exactly where the change happens, why it matters, what happens if it's skipped, what happens once it's done, and how to actually do it.

### Step 1: Pick One Method to Trust, Going Forward

**Where:** `scrapers/reviews_pipeline.py` and `scripts/adhd_sentiment_analysis.py`

**Why we need this:** Right now there are three different methods giving three different answers, and nobody has agreed on which one to actually believe. Without picking one, any number reported could be argued with, since someone could always point to a different method giving a different result.

**What happens if we don't:** Confusion continues every time someone asks "how many ADHD reviews do we have," the honest answer would be "it depends which method you ask," which isn't a usable answer for making decisions.

**What happens if we do:** Everyone including anyone outside this project looking at the numbers knows exactly which number to trust and why, since there's one clear, agreed method behind it.

**How to do it:** Formally agree that the word search method is the official method, and stop reporting or relying on numbers from the loose word search method as if they represented genuine ADHD reviews.

### Step 2: Clean Up Small Technical Issues That Get in the Way

**Where:** `scrapers/scrape_pipeline.py` (crashing issue) and `app.py` (frozen number issue, on the Summary page)

**Why we need this:** These are small, unrelated bugs I noticed while testing everything else. One causes the tool to crash unexpectedly on some computers before it even runs properly. The other means a number shown on the dashboard never actually updates it's stuck showing the same value regardless of what the real data says.

**What happens if we don't:** The tool remains unreliable to run on certain computers without extra workarounds, and the dashboard keeps quietly displaying an incorrect, outdated number to anyone who looks at it.

**What happens if we do:** The tool runs smoothly everywhere without needing special workarounds, and the dashboard always shows the real, current number instead of a frozen placeholder.

**How to do it:** In the first file, adjust the setting that controls how text is displayed so it doesn't crash on certain computers. In the second file, replace the fixed, typed-in number with a calculation that reads the real data and updates automatically.

### Step 3: Expand the Search for ADHD-Related Apps

**Where:** `scrapers/discovery.yml`

**Why we need this:** This file contains the list of words the app uses to go looking for new apps to analyze in the first place. Right now, only a very small number of those words are related to ADHD or neurodivergence at all most are just general productivity terms.

**What happens if we don't:** The app keeps only discovering a narrow set of apps, which limits how many ADHD-related reviews could ever be found, no matter how well the review-checking method works.

**What happens if we do:** The app discovers a wider range of relevant apps ones more likely to attract ADHD-related reviews in the first place giving the whole process a bigger, better starting pool to work with.

**How to do it:** Open this file and add a few more relevant words to the existing list, such as "autism," "neurodivergent," and "executive function," alongside the ones already there.

### Step 4: Collect a Larger Number of Reviews Overall

**Where:** `scrapers/reviews_pipeline.py` (this is adjusted through settings each time the tool is run, rather than a permanent code change)

**Why we need this:** At the moment, the tool only collects a small number of the most recent reviews per app, and only from a fairly short time window. That puts a hard ceiling on how much material there is to search through in the first place.

**What happens if we don't:** Even with every other fix in place, there simply won't be enough raw reviews available to realistically reach 2,000 genuine ADHD-related ones.

**What happens if we do:** There's a much larger pool of reviews overall, which directly increases the chances of finding more genuine ADHD-related ones among them.

**How to do it:** When running the review-collection tool, increase the setting that controls how many reviews are collected per app, and increase the setting that controls how far back in time it looks.

### Step 5: Carefully Add a Few More ADHD-Related Search Words

**Where:** `scrapers/reviews_pipeline.py` (the same list of words involved in the original fix)

**Why we need this:** The current list of recognized words is accurate but narrow. Real reviews describe ADHD-related experiences in many different ways, and some genuine ones are likely still being missed simply because they don't use one of the exact words currently on the list.

**What happens if we don't:** The method stays accurate but continues to miss some real ADHD-related reviews that are worded differently than expected.

**What happens if we do:** More genuine ADHD-related reviews get correctly recognized and counted, without reintroducing false matches.

**How to do it:** Add a small number of new, genuinely relevant words (such as "hyperfocus") to the list, testing each one individually against sample reviews first, to make sure it doesn't accidentally match an unrelated everyday word or phrase, the same way "add" once did.

### Step 6: Have a Real Person Manually Check a Sample of the Results

**Where:** the final review data the tool produces (for example, `data/curated/reviews.csv`, where each review is marked as ADHD-related or not)

**Why we need this:** No automated method, however well-fixed, should be blindly trusted without someone checking its actual output at least once. This step is what confirms the whole process is working correctly on real, final results not just in testing.

**What happens if we don't:** The final number relies entirely on trusting the code, with no independent confirmation meaning any remaining hidden mistake could go unnoticed.

**What happens if we do:** The final number becomes something that's been genuinely verified by a person, not just produced by a script, which makes it far more credible to report and rely on.

**How to do it:** Once the larger dataset has been collected and processed, take a random sample of the reviews marked as ADHD-related, read through them personally, and confirm they genuinely are what the system says they are.

------------------------------------------------------------------------

## Bottom Line

The real goal here isn't simply to make the number bigger, it's to make sure that every single review counted as "ADHD-related" genuinely is one, so that the final number of 2,000 actually means something meaningful and trustworthy. I found a real mistake that was causing some reviews to be counted incorrectly, fixed that mistake, carefully tested and confirmed that the fix works as intended, and used everything learned along the way to put together a clear, practical, step-by-step plan for responsibly and accurately growing that number toward 2,000.
