const { Client } = require("pg");
const axios = require("axios");
const dotenv = require("dotenv");

dotenv.config();

const client = new Client({
  user: process.env.USER,
  host: process.env.HOST,
  database: process.env.DATABASE,
  password: process.env.PASSWORD,
  port: process.env.PORT,
});

// Retrieve all Brazilian cities from the IBGE API
async function retrieveCities() {
  const url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios";

  try {
    const { data } = await axios.get(url);
    console.log(`Found ${data.length} cities`);
    return data.map((city) => ({
      name: city.nome.replace("'", " "),
      state: city.microrregiao.mesorregiao.UF.sigla,
      ibge_code: city.id,
    }));
  } catch (error) {
    console.error(error);
    return [];
  }
}

// Insert cities on database
async function insertCities(cities) {
  client
    .connect()
    .then(() => console.log("connected"))
    .catch((err) => console.error("connection error", err.stack));

  cities.forEach(async (city) => {
    try {
      await client.query(
        `INSERT INTO cities (name, state, ibge_code, created_at, updated_at) VALUES ('${city.name}', '${city.state}', ${city.ibge_code}, NOW(), NOW())`
      );
    } catch (error) {
      console.log(error);
      process.exit(1);
    }
  });
  await client.query("COMMIT");
  await client.release;
}

// Main program flow
async function main() {
  const cities = await retrieveCities();
  if (cities.length > 0) {
    await insertCities(cities);
    console.log("Cities entered successfully");
    process.exit(1);
  } else {
    console.error("No cities retrieved");
    process.exit(1);
  }
}

main();
