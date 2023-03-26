import { Home, Dashboard,ExitToApp, AddBox, AssessmentOutlined } from '@material-ui/icons';

export const sidemenu = [
    {
        name: "Home",
        path: "/home",
        icon: <Home />
    },
    {
        name: "Dashboard",
        path: "/dashboard",
        icon: <Dashboard />
    },
    {
        name: "Add Ticker",
        path: "/AddTicker",
        icon: <AddBox />
    },
    {
        name: "Scenarios",
        path: "/Scenarios",
        icon: <AssessmentOutlined />
    },
    {
        name: "Logout",
        path: "/logout",
        icon: <ExitToApp />
    },
]