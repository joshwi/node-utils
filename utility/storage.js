const fs = require('fs')
// const path = require("path")

function readDir(directory) {
    // let folder = path.join(__dirname, `/../../static/${directory}`)
    let folder = directory
    let output = fs.readdirSync(folder, (err, files) => { return files })
    return output
}

function readJson(filepath) {

    // let file = path.join(__dirname, `/../../static/${filepath}`)
    let file = filepath
    let output = `No such file or directory: ${file}`

    if(fs.existsSync(file)){
        output = fs.readFileSync(file, (err, data) => {
            if (err) return null;
            return data
        })
        try{ output = JSON.parse(output) } catch(error) { return null} 
    }

    return output
}

module.exports = {readDir, readJson}