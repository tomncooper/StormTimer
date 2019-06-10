import matplotlib.style
import matplotlib

matplotlib.style.use("ggplot")

import pandas as pd

keys = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"]
weights = [
    (6.0 / 32.0),
    (4.0 / 32.0),
    (3.0 / 32.0),
    (3.0 / 32.0),
    (2.0 / 32.0),
    (2.0 / 32.0),
    (2.0 / 32.0),
    (2.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
    (1.0 / 32.0),
]

key_weights = (
    pd.DataFrame(list(zip(keys, weights)))
    .rename(columns={0: "key", 1: "weight"})
    .set_index("key")
)

kw_ax = key_weights.plot(kind="bar", legend=False)

kw_ax.set(ylabel="Weight", xlabel="Key")

fig = kw_ax.get_figure()
fig.savefig("key-dist.pdf")
