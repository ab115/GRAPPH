import React, { Component, useState, useEffect } from 'react';
import { Line } from 'react-chartjs-2';
import Chart from 'chart.js/auto';
import '../../layout/layout.css';
import TickerSelection from '../../components/autoCombo';
import Button from '@material-ui/core/Button';
import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';
import { makeStyles } from '@material-ui/core/styles';
import { Grid } from '@material-ui/core';
import PortfolioData from './PortfolioData';

class AddTicker extends Component {
    constructor(props) {
        super(props);
        this.state = {ticker: "", tickerName: "", tickers:[]};
      }
    
      componentDidMount() {
           
      }
      
    handleSubmit = (event) => {
        event.preventDefault();
        fetch('http://localhost:5000/add_ticker?' + new URLSearchParams({
            pticker: this.state.ticker,
        }))
        .then(result => alert("Added Successfully"))       
   }
    
    handleChange = (value) => {
        this.state.ticker = value.code;
      }

      render() {   
        const { classes } = this.props;
        const tickers = [
            { code: 'AAPL', label: 'Apple Inc'},
            { code: 'TATAMOT', label: 'Tata Motors' },
            { code: 'SBIN', label: 'State Bank Of India' },
            { code: 'MARUTI', label: 'Maruti Suzuki India Ltd' },
            { code: 'INFY', label: 'Infosys Ltd' },    
        ];

        return (
            <form>
                <Grid container spacing={2}>
                    <Grid item md={4} >
                        <Autocomplete
                            id="ticker-select"
                            //value={value}
                            onChange= {(event, value) => this.handleChange(value)}
                            style={{ width: 500 }}
                            options={tickers}
                            
                            autoHighlight
                            getOptionLabel={(option) => option.label}
                            renderOption={(option) => (
                            <React.Fragment>
                                <span style={{marginRight: 15, fontSize: 18}}>{option.code}</span>
                                {option.label} ({option.code})
                            </React.Fragment>
                            )}
                            renderInput={(params) => (
                            <TextField
                                {...params}
                                label="Choose a ticker"
                                variant="outlined"
                                inputProps={{
                                ...params.inputProps,
                                autoComplete: '', // disable autocomplete and autofill
                                }}
                            />
                            )}
                        />
                    </Grid>
                    <Grid item md={4}>
                        <Button type="submit" variant="contained" onClick={this.handleSubmit}>Add to Portfolio</Button>
                    </Grid>
                    <Grid item md={4}></Grid>
                    <Grid item md={12}>
                        <PortfolioData></PortfolioData>
                    </Grid>
                </Grid>
            </form>
          );
      }
}

export default (AddTicker);