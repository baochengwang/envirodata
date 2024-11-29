function manualEnvirocode() {
    const date = document.getElementById('date').value;
    const address = document.getElementById('address').value;
    const outputElement = document.getElementById('manual_output');
    outputElement.innerHTML = "processing ...";
    fetch("/manual?" + new URLSearchParams({ date: date, address: address }), {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json'
        },
    })
        .then(response => response.json())
        .then(res => {
            console.log(res);
            outputElement.innerHTML = JSON.stringify(res, null, 2);
        })
        .catch(err => {
            console.log(err);
            outputElement.innerHTML = "Unsuccessful request";
        });
}
