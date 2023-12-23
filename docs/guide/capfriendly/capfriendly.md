---

icon: material/hat-fedora

---

# :material-hat-fedora: **capfriendly**

Information about the `capfriendly` module.

For in-depth materials, please consult the **[:material-bookshelf: Reference](../../reference/reference.md)**

## :fontawesome-solid-user-large: **Usage**

The `capfriendly` module and relevant functions can be imported using the below snippet:

```py
from chickenstats.capfriendly import scrape_capfriendly
```

The module is composed of only the one function, which scrapes CapFriendly data for a given year or years.

```py
capfriendly = scrape_capfriendly() # By default, returns the current season (2022)
```

## :material-tooltip-plus: **Tips**

The fine folks at CapFriendly are incredibly generous to provide such detailed data free of charge.
Please scrape responsibly, keeping in mind the service that CapFriendly provides to greater hockey 
community. 

!!! warning
    Save your data frequently to prevent unnecessary strain on the CapFriendly servers. 


