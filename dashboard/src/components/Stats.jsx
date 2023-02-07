import { useEffect, useState } from 'react'

const Stats = () => {
    const [event, setEvent] = useState([])

    useEffect(() => {
        fetch('http://localhost:8100/stats')
        .then(res => res.json())
        .then(res => { 
            setEvent(res) 
        })
    }, [])

    return (
        <div className="stats">
            <h2>Latest Statistics</h2>
            <div>
                <p> Last Updated: {event.last_updated}</p>
                <p> Max buy price: {event.max_buy_price}</p>
                <p>Max sell price: {event.max_sell_price}</p>
                <p>Num buys: {event.num_buys}</p>
                <p>Num sells: {event.num_sells}</p>
            </div>
        </div>
    )   
}

export default Stats