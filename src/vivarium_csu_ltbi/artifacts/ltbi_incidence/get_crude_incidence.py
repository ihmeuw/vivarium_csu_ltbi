import pandas as pd



def main():

    for country in ['ethiopia', 'india', 'peru', 'philippines', 'south_africa']:
        df = pd.read_hdf(country + '.hdf')
        print(f"{country} Mean:")
        print(df.loc[df.year_start == 2017].groupby(by=['sex']).mean())
        print(f"{country} UI:")
        print(df.loc[df.year_start == 2017].groupby(by=['sex']).quantile(q=0.025))
        print(df.loc[df.year_start == 2017].groupby(by=['sex']).quantile(q=0.975))


if __name__=="__main__":
    main()

