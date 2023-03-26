import React, { Component } from 'react'
import { Routes, Route, Navigate, Switch } from "react-router-dom"
import Home from "../pages/home/Home"
import Dashboard from "../pages/dashboard/Dashboard"
import Login from "../pages/login/Login"
import Logout from "../pages/logout/Logout"
import PNF from "../pages/404"
import AddTicker from '../pages/register/AddTicker'
export default class Router extends Component {
    render() {
        return (
            <React.Fragment>
                <Routes>
                    <Route exact path="/"  element={<Dashboard/>} />
                    <Route exact path="/home" element={<Home/>} />                  
                    <Route exact path="/dashboard"  element={<Dashboard/>}/>
                    <Route exact path="/AddTicker"  element={<AddTicker/>} />  
                    <Route exact path="/Scenarios"  element={<Logout/>} />  
                    <Route exact path="/Logout"  element={<Logout/>} />  
                                      
                    <Route exact path="/*" element={ PNF}/>
                </Routes>
            </React.Fragment>
        )
    }
}