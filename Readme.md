# Dream network exploration w/Gephi

## Install

```
npm i
```

## Generate data

`data/input.csv` is the input file.

```
npm run build
```

### Using the data

Take `.csv` files, they are node and edge sets (skip `input.csv`) and load them into Gephi.
Use Yifan-Hu layout, sizing by degree and coloring by `edgeTypes` and node `type`.
