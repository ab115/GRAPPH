import React, { Component, useState, useEffect } from 'react';
import TextField from '@material-ui/core/TextField';
import Autocomplete from '@material-ui/lab/Autocomplete';
import { makeStyles } from '@material-ui/core/styles';

const useStyles = makeStyles({
    option: {
      fontSize: 15,
      '& > span': {
        marginRight: 10,
        fontSize: 18,
      },
    },
  });
  
  export default function TickerSelection (props) {
    const classes = useStyles();
    const [value, setValue] = React.useState('');
  
    var handleChange = this.props.handleChange;

    return (
      <Autocomplete
        id="ticker-select"
        value={value}
        onChange={(event, newValue) => {
          setValue(newValue);
          handleChange(newValue);
        }}
        style={{ width: 300 }}
        options={tickers}
        classes={{
          option: classes.option,
        }}
        autoHighlight
        getOptionLabel={(option) => option.label}
        renderOption={(option) => (
          <React.Fragment>
            <span>{option.code}</span>
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
              autoComplete: 'new-password', // disable autocomplete and autofill
            }}
          />
        )}
      />
    );
  }
  
  const tickers = [
    { code: 'APPL', label: 'Apple Inc'},
    { code: 'TATAMOT', label: 'Tata Motors' },
    { code: 'SBIN', label: 'State Bank Of India' },
    { code: 'MARUTI', label: 'Maruti Suzuki India Ltd' },
    { code: 'INFY', label: 'Infosys Ltd' },    
  ];
  