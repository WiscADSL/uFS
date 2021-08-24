This demo application provides an example of how to process scopes and
variables that contain the "uid-" prefix. The "recipes.cfg" file
contains a collection of "uid-recipe" scopes, like those shown below.

----start of recipes.cfg----
uid-recipe {
    name = "Tea";
    ingredients = ["1 tea bag", "cold water", "milk"];
    uid-step = "Pour cold water into the kettle";
    uid-step = "Turn on the kettle";
    uid-step = "Wait for the kettle to boil";
    uid-step = "Pour boiled water into a cup";
    uid-step = "Add tea bag to cup & leave for 3 minutes";
    uid-step = "Remove tea bag";
    uid-step = "Add a splash of milk if you want";
}
uid-recipe {
    name = "Toast";
    ingredients = ["Two slices of bread", "butter"];
    uid-step = "Place bread in a toaster and turn on";
    uid-step = "Wait for toaster to pop out the bread";
    uid-step = "Remove bread from toaster and butter it";
}
----end of recipes.cfg----

The "RecipeFileParser" class provides a simple API for parsing such a
file and iterating over the recipes contained within it. The parse()
operation within this class illustrates how to perform schema validation
for scopes and variables that contain the "uid-" prefix. In addition,
the parse() and getRecipeSteps() operations illustrate how to use pass a
filter string such as "uid-recipe" or "uid-step" to
listFullyScopedNames() and listLocallyScopedNames() to get a list of
"uid-" entries.

Examples of running the demo:

    demo -h                     (prints a usage statement)
    demo -recipes recipes.cfg
