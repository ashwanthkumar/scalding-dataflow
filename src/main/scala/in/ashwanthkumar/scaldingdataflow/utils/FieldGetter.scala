package in.ashwanthkumar.scaldingdataflow.utils

import java.lang.reflect.{Field => JField}
import java.util

import com.google.api.client.util.Maps

class FieldGetter(clazz: Class[_]) {
  private val fields: util.Map[String, JField] = Maps.newHashMap[String, JField]()

  clazz.getDeclaredFields.foreach(f => {
    f.setAccessible(true)
    this.fields.put(f.getName, f)
  })

  def get[T](fieldname: String, value: AnyRef): T = {
    fields.get(fieldname).get(value).asInstanceOf[T]
  }
}
