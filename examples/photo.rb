require 'active_document'

class Photo < ActiveDocument::Base
  path '/var/data'

  accessor :id, :tagged_ids, :user_id, :description

  index_by :tagged_ids  
end
