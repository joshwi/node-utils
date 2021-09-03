const _ = require("underscore")

async function connectionStatus(driver) {
    let output = { message: `Neo4j Connection Status`, status: "Unknown" }

    try {
        await driver.verifyConnectivity()
        output = { message: `Neo4j Connection Status`, status: "Connected" }
    } catch (error) {
        output = { message: `Neo4j Connection Status`, status: "Failed", error: error }
    }

    return output
}

async function runCypher(driver, cypher, correlationID) {
    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] runCypher - Start`)

    const session = driver.session()

    if (cypher) {
        try {
            let response = await session.run(cypher, {})
            let records = response.records
            let stats = response.summary && response.summary.counters && response.summary.counters._stats ? response.summary.counters._stats : {}
            records = records.map(entry => {
                let fields = entry._fields.map(index => { if (index && index.properties) { return index.properties } else { return index } })
                return _.object(entry.keys, fields)
            })
            date = new Date().toISOString()
            console.log(`[ ${date} ] [ ${correlationID} ] runCypher - Finish`)
            let output = { records: records, stats: stats }
            return output
        } catch (error) {
            date = new Date().toISOString()
            console.log(`[ ${date} ] [ ${correlationID} ] runCypher - ${error}`)
            return { error: { code: error.code, message: error.message } }
        }
    }
    return {}
}

async function runTransactions(driver, commands, correlationID) {

    const session = driver.session()

    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] runTransactions - Start`)

    let tx = session.beginTransaction()

    let promises = commands.map(async (cypher, id) => { return tx.run(cypher).catch(error => { return { id: id, error: error } }) })

    let result = await Promise.all(promises)

    let status = await tx.commit().then(() => { return undefined }).catch(err => { return err })

    let output = session.lastBookmark().pop()

    if (status) {
        output = result.filter(x => x.error).shift()
        date = new Date().toISOString()
        console.log(`[ ${date} ] [ ${correlationID} ] [ runTransactions ] - Failed - ${output.error}`)
    } else {
        date = new Date().toISOString()
        console.log(`[ ${date} ] [ ${correlationID} ] runTransactions - Finish`)
    }

    return output

}

async function getNode(driver, node, query, correlationID) {

    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] getNode - Start`)

    const session = driver.session()

    let result = []

    let cypher = `MATCH (${node ? "n:" + node : "n"}) ${query.filter ? "WHERE " + query.filter : ""} RETURN `

    if (query.fields && query.fields.length > 0) {
        query.fields.map((entry, index) => {
            if(entry.indexOf("count_") > -1){
                index === 0 ? cypher += `count(${entry.split("_").pop()}) as ${entry}` : cypher += `, count(${entry.split("_").pop()}) as ${entry}`
            }
            else if(entry.indexOf("distinct_") > -1){
                index === 0 ? cypher += `distinct(n.${entry.split("_").pop()}) as ${entry}` : cypher += `, distinct(n.${entry.split("_").pop()}) as ${entry}`
            }else{
                index === 0 ? cypher += `n.${entry} as ${entry}` : cypher += `, n.${entry} as ${entry}`
            }
        })
    } else {
        cypher += "n"
    }

    if (query.limit) { cypher += ` LIMIT ${query.limit}` }

    let output = await session.run(cypher, {}).then(response => {
        response.records.forEach(record => {
            if (query.fields && query.fields.length > 0) {
                let data = {}
                query.fields.map((index) => { data[index] = record.get(index) })
                result.push(data)
            }
            else {
                let data = _.omit(record.get("n").properties, ["_id", "_labels"])
                result.push(data)
            }
        })
    }).catch(err => { 
        date = new Date().toISOString()
        console.log(`[ ${date} ] [ ${correlationID} ] getNode - Failed ${err}`)
    }).then(() => { session.close(); return result; })


    date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] getNode - Finish`)

    return output
}

async function postNode(driver, node, label, properties, correlationID) {

    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Start`)

    let cypher = `CREATE (n:${node} {label:"${label}"})`

    Object.keys(properties).map(entry => {
        cypher += ` SET n.${entry}="${properties[entry]}"`
    })

    console.log(cypher)

    let output = await runCypher(driver, cypher, correlationID)
    output = output.stats ? output.stats : output

    date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Finish`)

    return output
}

async function putNode(driver, node, label, properties, correlationID) {

    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Start`)

    let cypher = `MERGE (n:${node} {label:"${label}"})`

    Object.keys(properties).map(entry => {
        cypher += ` SET n.${entry}="${properties[entry]}"`
    })

    console.log(cypher)

    let output = await runCypher(driver, cypher, correlationID)
    output = output.stats ? output.stats : output

    date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Finish`)

    return output
}

async function deleteNode(driver, node, label, correlationID) {

    let date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Start`)

    let cypher = `MATCH (n:${node})`

    if(label){
        cypher += ` WHERE n.label="${label}" DELETE n`
    }else{
        cypher += ` DELETE n`
    }

    console.log(cypher)

    let output = await runCypher(driver, cypher, correlationID)
    output = output.stats ? output.stats : output

    date = new Date().toISOString()
    console.log(`[ ${date} ] [ ${correlationID} ] postNode - Finish`)

    return output
}

module.exports = { connectionStatus, runCypher, runTransactions, getNode, postNode, putNode, deleteNode }