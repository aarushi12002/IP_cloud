package appEngine.controller;

import java.util.Date;
import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;

import appEngine.model.Sensor;
import appEngine.util.PMF;

@Controller
@RequestMapping("/sensor")
public class SensorController {

	@RequestMapping(value = "/add", method = RequestMethod.GET)
	public String getAddSensorPage(ModelMap model) {

		return "add";

	}

	@RequestMapping(value = "/add", method = RequestMethod.POST)
	public ModelAndView add(HttpServletRequest request, ModelMap model) {

		String name = request.getParameter("uuid");
		String email = request.getParameter("value");

		Sensor c = new Sensor();
		c.setUuid(name);
		c.setValue(Float.parseFloat(email));
		c.setDate(new Date());

		PersistenceManager pm = PMF.get().getPersistenceManager();
		try {
			pm.makePersistent(c);
		} finally {
			pm.close();
		}

		return new ModelAndView("redirect:list");

	}

	@RequestMapping(value = "/update/{name}", method = RequestMethod.GET)
	public String getUpdateSensorPage(@PathVariable String name,
			HttpServletRequest request, ModelMap model) {

		PersistenceManager pm = PMF.get().getPersistenceManager();

		Query q = pm.newQuery(Sensor.class);
		q.setFilter("name == nameParameter");
		q.setOrdering("date desc");
		q.declareParameters("String nameParameter");

		try {
			List<Sensor> results = (List<Sensor>) q.execute(name);

			if (results.isEmpty()) {
				model.addAttribute("sensor", null);
			} else {
				model.addAttribute("sensor", results.get(0));
			}
		} finally {
			q.closeAll();
			pm.close();
		}

		return "update";

	}

	@RequestMapping(value = "/update", method = RequestMethod.POST)
	public ModelAndView update(HttpServletRequest request, ModelMap model) {

		String name = request.getParameter("uuid");
		String email = request.getParameter("value");
		String key = request.getParameter("key");

		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {

			Sensor c = pm.getObjectById(Sensor.class, key);

			c.setUuid(name);
			c.setValue(Float.parseFloat(email));
			c.setDate(new Date());

		} finally {

			pm.close();
		}

		// return to list
		return new ModelAndView("redirect:list");

	}

	@RequestMapping(value = "/delete/{key}", method = RequestMethod.GET)
	public ModelAndView delete(@PathVariable String key,
			HttpServletRequest request, ModelMap model) {

		PersistenceManager pm = PMF.get().getPersistenceManager();

		try {

			Sensor c = pm.getObjectById(Sensor.class, key);
			pm.deletePersistent(c);

		} finally {
			pm.close();
		}

		// return to list
		return new ModelAndView("redirect:../list");

	}


	@RequestMapping(value = "/list", method = RequestMethod.GET)
	public String listSensor(ModelMap model) {

		PersistenceManager pm = PMF.get().getPersistenceManager();
		Query q = pm.newQuery(Sensor.class);
		q.setOrdering("date desc");

		List<Sensor> results = null;

		try {
			results = (List<Sensor>) q.execute();

			if (results.isEmpty()) {
				model.addAttribute("sensorList", null);
			} else {
				model.addAttribute("sensorList", results);
			}

		} finally {
			q.closeAll();
			pm.close();
		}

		return "list";

	}

}