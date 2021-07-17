const neo4j = require("./utility/neo4j")

// const neo4j = require("neo4j-driver")
// const driver = neo4j.driver("bolt://localhost:7687", neo4j.auth.basic("neo4j", "nico"), { disableLosslessIntegers: true })
// let output = test.getNode(driver, "games", { filter: "n.homeTag='kan'", fields: ["distinct_awayTeam", "count_n"], limit: 1000 })

module.exports = {neo4j}