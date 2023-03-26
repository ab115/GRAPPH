import React, { useState, useRef, useEffect, useMemo, useCallback} from 'react';
import { render } from 'react-dom';
import { AgGridReact } from 'ag-grid-react'; // the AG Grid React Component
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-alpine.css';
import PortfolioVsBenchmark from '../Charts/portfolioVsBenchmark';
import Grid from '@material-ui/core/Grid';
import Paper from "@material-ui/core/Paper";
import '../../layout/layout.css';


const Dashboard = () => {
    const gridRef = useRef(); // Optional - for accessing Grid's API
    const [rowData, setRowData] = useState(); // Set rowData to Array of Objects, one Object per Row
   
    // Each Column Definition results in one Column.
    const [columnDefs, setColumnDefs] = useState([
      {field: 'symbol', filter: true},
      {field: 'socialScore', filter: true},
      {field: 'governanceScore'},
      {field: 'environmentScore', filter: true}, 
      {field: 'totalEsg', filter: true}, 
      {field: 'esgPerformance', filter: true}, 
      {field: 'percentile', filter: true}, 
      {field: 'peerGroup', filter: true}, 
      {field: 'highestControversy' , filter: true}
    ]);

    // DefaultColDef sets props common to all Columns
    const defaultColDef = useMemo( ()=> ({
        sortable: true
      }));
   
    // Example of consuming Grid Event
    const cellClickedListener = useCallback( event => {
      //console.log('cellClicked', event);
    }, []);
   
    // Example load data from sever
    useEffect(() => {
      fetch('http://localhost:5000/get_esg_scores')
      .then(result => result.json())      
      .then(rowData => setRowData(rowData))
    }, []);
   
    // Example using Grid's API
    const buttonListener = useCallback( e => {
      gridRef.current.api.deselectAll();
    }, []);
   
    return (
      <Grid container spacing={2}>
        <Grid item md={6} >
          <Paper>
            <div className='halfHeight-div'>
              <PortfolioVsBenchmark></PortfolioVsBenchmark>
            </div>
          </Paper>
        </Grid>
        <Grid item md={6}>
          <Paper >
            <div className="ag-theme-alpine">   
              {/* On div wrapping Grid a) specify theme CSS Class Class and b) sets Grid size */}
              <div className="ag-theme-alpine" style={{height: '88vh'}}>        
                <AgGridReact
                    ref={gridRef} // Ref for accessing Grid's API
        
                    rowData={rowData} // Row Data for Rows
        
                    columnDefs={columnDefs} // Column Defs for Columns
                    defaultColDef={defaultColDef} // Default Column Properties
        
                    animateRows={true} // Optional - set to 'true' to have rows animate when sorted
                    rowSelection='multiple' // Options - allows click selection of rows
        
                    onCellClicked={cellClickedListener} // Optional - registering for Grid Event
                    />
              </div>
            </div>
          </Paper>
      </Grid>
    </Grid>
    );
   };
   
   export default Dashboard;