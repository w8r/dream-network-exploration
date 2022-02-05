import { readFile, writeFile } from "fs/promises";
import fs from "fs";
import path from "path";
import * as csv from "csv";

const input = await readFile(
  path.join(process.cwd(), "data", "input.csv"),
  "utf8"
);

const parser = csv.parse({ columns: true }, (err, records) => {
  console.log(records);
});

//fs.createReadStream(__dirname + "/chart-of-accounts.csv").pipe(parser);
//console.log(parser);

//parser.read(input);
const data = [];
fs.createReadStream(path.join(process.cwd(), "data", "input.csv"))
  .pipe(csv.parse({ delimiter: "," }))
  .on("data", ([x, readId, place, intExt, actors, age, sex, covid, stress]) => {
    const rec = {
      id: `r${data.length}`,
      type: "DREAM",
      Label: `Dream #${data.length}`,
      place,
      intExt,
      actors,
      age,
      sex,
      covid,
      stress,
    };
    data.push(rec);
  })
  .on("end", () => {
    data.shift();
    // create person nodes
    const personHasDream = [];
    const persons = data.reduce((acc, { id, age, sex, covid, stress }, i) => {
      const personId = `p${i}`;
      personHasDream.push({
        source: personId,
        target: id,
        edgeType: "HAS_DREAM",
      });
      acc.push({
        age,
        sex,
        covid,
        stress,
        id: personId,
        type: "PERSON",
        Label: `${sex}${age}`,
      });
      return acc;
    }, []);

    const placeToDream = [];
    let l = 0;
    const placeToId = new Map();
    const places = data.reduce((acc, { place, intExt, id: dreamId }) => {
      const locations = place.split(";").map((location) => location.trim());

      locations.forEach((location) => {
        intExt = intExt === "0" ? undefined : intExt;
        location = location === "0" || location === "" ? undefined : location;
        const hash = `${location}, ${intExt}`;
        if (!placeToId.has(hash)) {
          placeToId.set(hash, `l${l++}`);
          acc.push({
            location,
            intExt,
            id: placeToId.get(hash),
            Label: `${location}${intExt ? ` (${intExt})` : ""}`,
            type: "LOCATION",
          });
        }
        placeToDream.push({
          source: placeToId.get(hash),
          target: dreamId,
          edgeType: "LOCATION_OF_DREAM",
        });
      });
      return acc;
    }, []);

    const dreamHasActor = [];
    let a = 0;
    const actorToId = new Map();
    const actors = data.reduce((acc, { actors, id: dreamId }) => {
      const actorsList = actors.split(";").map((actor) => actor.trim());
      actorsList.forEach((actor) => {
        if (!actorToId.has(actor)) {
          actorToId.set(actor, `a${a++}`);
          acc.push({
            name: actor || undefined,
            id: actorToId.get(actor),
            type: "ACTOR",
            Label: actor,
          });
        }
        dreamHasActor.push({
          source: dreamId,
          target: actorToId.get(actor),
          edgeType: "HAS_ACTOR",
        });
      });
      return acc;
    }, []);

    const ageGroups = [
      {
        minAge: 0,
        maxAge: 20,
      },
      {
        minAge: 21,
        maxAge: 34,
      },
      {
        minAge: 35,
        maxAge: 49,
      },
      {
        minAge: 50,
        maxAge: 60,
      },
      {
        minAge: 61,
        maxAge: 70,
      },
    ];

    ageGroups.forEach((ag, i) => {
      ag.id = `ag${i}`;
      ag.Label = `${ag.minAge} - ${ag.maxAge}`;
      ag.type = "AGE_GROUP";
    });

    const personHasAgeGroup = persons.reduce((acc, { id, age }) => {
      const ageGroup = ageGroups.find(
        (ag) => ag.minAge <= age && age <= ag.maxAge
      );
      acc.push({
        source: id,
        target: ageGroup.id,
        edgeType: "HAS_AGE_GROUP",
      });
      return acc;
    }, []);

    const placeToAgeGroup = places.reduce((acc, { id }) => {
      placeToDream
        .filter(({ source }) => source === id)
        .forEach(({ target }) => {
          const dream = data.find(({ id }) => id === target);
          //console.log(id, "->", dream.id, dream.age);

          const age = dream.age;
          const ageGroup = ageGroups.find(
            ({ minAge, maxAge }) => minAge <= age && age <= maxAge
          );

          acc.push({
            source: id,
            target: ageGroup.id,
            edgeType: "LOCATION_TO_AGE_GROUP",
          });
        });

      // find the dream for the place
      // get the age out of it
      // find the age group for the age
      // const gream = d;
      // const ageGroup = ageGroups.find(
      //   (ag) => ag.minAge <= age && age <= ag.maxAge
      // );
      // acc.push({
      //   source: id,
      //   target: ageGroup.id,
      //   edgeType: "LOCATION_OF_AGE_GROUP",
      // });
      return acc;
    }, []);

    const actorToAgeGroup = actors.reduce((acc, { id }) => {
      dreamHasActor.forEach(({ source, target }) => {
        if (target === id) {
          const { age } = data.find(({ id }) => id === source);
          const ageGroup = ageGroups.find(
            ({ minAge, maxAge }) => minAge <= age && age <= maxAge
          );
          acc.push({
            source: id,
            target: ageGroup.id,
            edgeType: "ACTOR_TO_AGE_GROUP",
          });
        }
      });
      return acc;
    }, []);

    //console.log(placeToAgeGroup);

    Promise.all([
      writeTable(persons, path.join(process.cwd(), "data", "persons.csv")),
      writeTable(places, path.join(process.cwd(), "data", "places.csv")),
      writeTable(actors, path.join(process.cwd(), "data", "actors.csv")),
      writeTable(
        personHasDream,
        path.join(process.cwd(), "data", "personHasDream.csv")
      ),
      writeTable(
        placeToDream,
        path.join(process.cwd(), "data", "placeToDream.csv")
      ),
      writeTable(
        dreamHasActor,
        path.join(process.cwd(), "data", "dreamHasActor.csv")
      ),
      writeTable(data, path.join(process.cwd(), "data", "dreams.csv")),
      writeTable(ageGroups, path.join(process.cwd(), "data", "ageGroups.csv")),
      writeTable(
        personHasAgeGroup,
        path.join(process.cwd(), "data", "personHasAgeGroup.csv")
      ),
      writeTable(
        placeToAgeGroup,
        path.join(process.cwd(), "data", "placeToAgeGroup.csv")
      ),
      writeTable(
        actorToAgeGroup,
        path.join(process.cwd(), "data", "actorToAgeGroup.csv")
      ),
    ]).then(() => console.log("written"));
  });

function writeTable(data, filename) {
  const columns = Object.keys(data[0]);
  return new Promise((resolve, reject) => {
    csv.stringify(
      data,
      {
        header: true,
        columns,
      },
      (err, output) => {
        if (err) return reject(err);
        resolve(output);
      }
    );
  }).then((output) => writeFile(filename, output));
}
