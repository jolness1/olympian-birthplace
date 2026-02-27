import pandas as pd

df = pd.read_csv('extra-data/montana-events.csv')

df['Medal'] = df['Medal'].str.strip()

medals = df[df['Medal'].isin(['Gold', 'Silver', 'Bronze'])]

medal_counts = medals['Medal'].value_counts().reindex(['Gold', 'Silver', 'Bronze'], fill_value=0).reset_index()
medal_counts.columns = ['medal', 'number']
medal_counts['medal'] = medal_counts['medal'].str.lower()
medal_counts.to_csv('extra-data/medal-counts.csv', index=False)

athlete_medals = (
    medals.groupby('As')['Medal']
    .value_counts()
    .unstack(fill_value=0)
    .reindex(columns=['Gold', 'Silver', 'Bronze'], fill_value=0)
    .reset_index()
    .rename(columns={'As': 'name', 'Gold': 'gold', 'Silver': 'silver', 'Bronze': 'bronze'})
)
athlete_medals.to_csv('extra-data/athlete-medals.csv', index=False)

print("Medal counts:")
print(medal_counts.to_string(index=False))
print("\nAthlete medals:")
print(athlete_medals.to_string(index=False))
