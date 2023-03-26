import React, { Component, useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import Chart from 'chart.js/auto';

class Home extends Component {
  constructor(props) {
    super(props);
    this.state = {grDataBM: [], grDataPF: [], grLabels: []};
  }

  componentDidMount() {
    fetch("http://127.0.0.1:5000/do_panalysis")
    .then((res) => res.json()
    .then((data) => {
            this.setState({ grDataBM: data.gDataBM, 
                            grDataPF: data.gDataPF,
                            grLabels: data.gLabels })                        
          })
      );     
  }
    
  render() {
    let gData = {
      labels: this.state.grLabels,
      datasets: [        
          {
            label: 'Portfolio',
            data: this.state.grDataPF,
            backgroundColor: 'rgba(255, 99, 132, 0.5)',
            fill: false,
            borderColor: 'rgb(255, 99, 132)',
            tension: 0.1
          },
          {
            label: 'Benchmark',
            data: this.state.grDataBM,
            backgroundColor: 'rgba(53, 162, 235, 0.5)',
            fill: false,
            borderColor: 'rgb(75, 192, 192)',
            tension: 0.1
          },
      ]
    };
    
    let options = {
      scales: {
        y: {
          beginAtZero: true
        }
      }
    };

    return (
      <div>
          <Line data={gData} options={options} />
      </div>
    );
  }
}

export default Home;