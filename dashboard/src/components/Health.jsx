import {useState } from 'react'

const Stats = () => {
    const [event, setEvent] = useState([])
    const checkStatus = () => {
        fetch('http://localhost:8110/check')
        .then(res => res.json())
        .then(res => { 
            setEvent(res)
        })
        .then(()=>{
            console.log(event)
        })
        .catch((err)=>{
            console.log(err)
        })
    }
    return (
        <div >
            <h2>Health Check</h2>
            <div>
                <p> Processing status: {event.processing}</p>
                <p> Receiver status: {event.receiver}</p>
                <p> Storage status: {event.storage}</p>
            </div>
            <button onClick={() => checkStatus()}>Update status</button>
        </div>
    )   
}

export default Stats
