class ActiveDocument::Attribute
  attr_reader :name, :opts

  def initialize(name, opts = {})
    @name = name
    @opts = opts
  end

  def type
    opts[:type]
  end

  def reader
    attr = self
    case type
    when Time
      lambda do
        attributes[attr.name] 
      end
    else
    end
  end

  def writer
    attr = self
    lambda do |value|
      attributes[attr.name] = value
    end
  end
end

#     if type = opts[:type]
#       # Need to cast the attributes.
#           return unless attributes[attr]
#           return attributes[attr] if attributes[attr].kind_of?(type)

#           case type
#           when Date
#             attributes[attr] = Date.parse(attributes[attr])
#           when Time
#             attributes[attr] = Time.parse(attributes[attr])
#           when ActiveDocument::Base, ActiveRecord::Base
#             type.find_by_id(attributes["#{attr}_id"])                                     
#           else
#             attributes[attr] = type.new(attributes[attr])
#           end
#         end
#       end
#     else
#       # Just return the raw attribute values.
#       attrs.each do |attr|
#         define_method(attr) do
#           attributes[attr]
#         end
#       end
#     end
#   end
