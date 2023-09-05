const express = require('express')
const app = express()
const port = 3000

app.get('/', (req, res) => {
    res.send('Hello my image, im executed a test of software!')    
})

app.listen(port, () => {
    console.log(`Executando na porta: ${port}`)
});