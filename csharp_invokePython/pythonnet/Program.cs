using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Python.Runtime;


namespace testpythonnet
{
    class Program
    {
        static void Main(string[] args)
        {
            PythonEngine.Initialize();
            using (Py.GIL())
            {
                //Creating module object
                PyObject testClassModule = PythonEngine.ImportModule("Iron_hello");

                //Calling module method
                PyObject result = testClassModule.InvokeMethod("test");
                Console.WriteLine("Test method result = {0}", result.ToString());

                //dynamic classTest = testClassModule.GetAttr("ClassTest");
                //dynamic tmp = classTest();
                //tmp.Initalize();
                //tmp.Execute();
            }
            PythonEngine.Shutdown();
        }


    }
}
